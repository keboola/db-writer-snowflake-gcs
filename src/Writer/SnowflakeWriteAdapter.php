<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\ODBC\OdbcWriteAdapter;
use Keboola\DbWriterAdapter\Query\QueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use RuntimeException;
use SplFileInfo;
use Symfony\Component\Process\Process;

/**
 * @property-read SnowflakeQueryBuilder $queryBuilder
 */
class SnowflakeWriteAdapter extends OdbcWriteAdapter
{
    use QuoteTrait;

    private Temp $tempDir;

    private SplFileInfo $snowSqlConfig;

    public function __construct(
        SnowflakeConnection $connection,
        QueryBuilder $queryBuilder,
        LoggerInterface $logger,
    ) {
        parent::__construct($connection, $queryBuilder, $logger);

        $this->tempDir = new Temp('wr-snowflake-adapter');
    }

    public function getName(): string
    {
        return 'Snowsql';
    }

    public function writeData(string $tableName, ExportConfig $exportConfig): void
    {
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $exportConfig->getDatabaseConfig();

        $this->snowSqlConfig = $this->createSnowSqlConfig($databaseConfig);

        // Upload to internal stage
        $this->logger->info(sprintf('Uploading data to internal stage "@~/%s"', $tableName));
        $this->putIntoInternalStage($exportConfig, $tableName);

        try {
            $items = array_filter(
                $exportConfig->getItems(),
                fn(ItemConfig $item) => strtolower($item->getType()) !== 'ignore',
            );

            // Copy from internal stage to staging table
            $this->logger->info(sprintf('Copying data from internal stage to staging table "%s"', $tableName));
            $query = $this->generateCopyQuery($exportConfig, $tableName, $items);
            $this->logger->debug($query);
            $this->connection->exec($query);
        } finally {
            $this->cleanupInternalStage($tableName);
        }
    }

    private function putIntoInternalStage(ExportConfig $exportConfig, string $tmpTableName): void
    {
        $putSql = $this->generatePutQuery($exportConfig, $tmpTableName);

        $sqlFile = $this->tempDir->createTmpFile('snowsql.sql');
        file_put_contents($sqlFile->getPathname(), $putSql);

        $command = sprintf(
            'snowsql --config %s -c writer -f %s',
            $this->snowSqlConfig,
            $sqlFile,
        );

        $this->logger->debug($putSql);
        $this->logger->debug(trim($command));

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
            throw new RuntimeException(sprintf(
                'File upload error occurred processing [%s]',
                $exportConfig->getTableFilePath(),
            ));
        }
    }

    private function cleanupInternalStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->connection->exec($sql);
    }

    public function generatePutQuery(ExportConfig $exportConfig, string $tmpTableName): string
    {
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $exportConfig->getDatabaseConfig();

        $warehouse = $databaseConfig->hasWarehouse() ? $databaseConfig->getWarehouse() : null;
        $database = $databaseConfig->getDatabase();
        $schema = $databaseConfig->hasSchema() ? $databaseConfig->getSchema() : null;

        $sql = [];
        if ($warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', $this->quoteIdentifier($warehouse));
        }

        $sql[] = sprintf('USE DATABASE %s;', $this->quoteIdentifier($database));

        if ($schema) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                $this->quoteIdentifier($database),
                $this->quoteIdentifier($schema),
            );
        }

        $sql[] = sprintf(
            'PUT file://%s @~/%s;',
            $exportConfig->getTableFilePath(),
            $tmpTableName,
        );

        return trim(implode("\n", $sql));
    }

    /**
     * @param array<ItemConfig> $items
     */
    public function generateCopyQuery(ExportConfig $exportConfig, string $tmpTableName, array $items): string
    {
        $csvOptions = [
            'SKIP_HEADER = 1',
            sprintf('FIELD_DELIMITER = %s', $this->quote(',')),
            sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote('"')),
            sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $this->quote('\\')),
            sprintf('COMPRESSION = %s', $this->quote('GZIP')),
        ];

        $tmpTableNameWithSchema = sprintf(
            '%s.%s',
            $this->quoteIdentifier($exportConfig->getDatabaseConfig()->getSchema()),
            $this->quoteIdentifier($tmpTableName),
        );

        return sprintf('
            COPY INTO %s(%s)
            FROM @~/%s
            FILE_FORMAT = (TYPE=CSV %s);',
            $tmpTableNameWithSchema,
            implode(', ', $this->quoteManyIdentifiers($items, fn(ItemConfig $column) => $column->getDbName())),
            $tmpTableName,
            implode(' ', $csvOptions),
        );
    }

    public function upsert(ExportConfig $exportConfig, string $stageTableName): void
    {
        $this->logger->info(sprintf('Upserting data to table "%s"', $exportConfig->getDbName()));
        if ($exportConfig->hasPrimaryKey()) {
            $this->addPrimaryKeyIfMissing($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
            $this->checkPrimaryKey($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
        }

        parent::upsert($exportConfig, $stageTableName);
    }

    public function swapTable(SnowflakeConnection $connection, string $tableName, string $stagingTableName): void
    {
        $this->logger->info(sprintf('Swapping table "%s" with "%s"', $stagingTableName, $tableName));
        $connection->exec(sprintf(
            'ALTER TABLE %s SWAP WITH %s',
            $this->quoteIdentifier($stagingTableName),
            $this->quoteIdentifier($tableName),
        ));
    }

    /**
     * @return array{Field: string, Type: string}[]
     */
    public function getTableInfo(string $tableName): array
    {
        /** @var array{name: string, type: string}[] $res */
        $res = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
        );

        return array_map(fn(array $item) => [
            'Field' => (string) $item['name'],
            'Type' => (string) $item['type'],
        ], $res);
    }

    public function validateTable(string $tableName, array $items): void
    {
        // turn off validation
    }

    public function getPrimaryKeys(string $tableName): array
    {
        $sqlPrimaryKeysInDb = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
        );
        return array_filter($sqlPrimaryKeysInDb, fn($v) => $v['primary key'] === 'Y');
    }

    private function addPrimaryKeyIfMissing(array $primaryKeys, string $tableName): void
    {
        $primaryKeysInDb = $this->getPrimaryKeys($tableName);

        if (!empty($primaryKeysInDb)) {
            return;
        }

        $this->connection->exec(
            $this->queryBuilder->addPrimaryKeyQueryStatement($this->connection, $tableName, $primaryKeys),
        );
    }

    private function checkPrimaryKey(array $primaryKeys, string $tableName): void
    {
        $primaryKeysInDb = $this->getPrimaryKeys($tableName);
        $primaryKeysInDb = array_map(fn(array $item) => $item['name'], $primaryKeysInDb);

        sort($primaryKeysInDb);
        sort($primaryKeys);

        if ($primaryKeysInDb !== $primaryKeys) {
            throw new UserException(sprintf(
                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
                . 'Keys in configuration: %s' . PHP_EOL
                . 'Keys in DB table: %s',
                implode(',', $primaryKeys),
                implode(',', $primaryKeysInDb),
            ));
        }
    }

    private function createSnowSqlConfig(SnowflakeDatabaseConfig $databaseConfig): SplFileInfo
    {
        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.writer]';
        $cliConfig[] = sprintf('accountname = "%s"', self::getAccountUrlFromHost($databaseConfig->getHost()));
        $cliConfig[] = sprintf('username = "%s"', $databaseConfig->getUser());
        $cliConfig[] = sprintf('password = "%s"', $databaseConfig->getPassword());
        $cliConfig[] = sprintf('dbname = "%s"', $databaseConfig->getDatabase());

        if ($databaseConfig->hasWarehouse()) {
            $cliConfig[] = sprintf('warehousename = "%s"', $databaseConfig->getWarehouse());
        }

        if ($databaseConfig->hasSchema()) {
            $cliConfig[] = sprintf('schemaname = "%s"', $databaseConfig->getSchema());
        }

        $file = $this->tempDir->createFile('snowsql.config');
        file_put_contents($file->getPathname(), implode("\n", $cliConfig));

        return $file;
    }

    private static function getAccountUrlFromHost(string $host): string
    {
        $hostParts = explode('.', $host);
        return implode('.', array_slice($hostParts, 0, count($hostParts) - 2));
    }
}
