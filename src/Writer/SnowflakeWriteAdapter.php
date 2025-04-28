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
            $query = $this->queryBuilder->copyIntoTableQueryStatement($this->connection, $tableName, $items);
            $this->connection->exec($query);
        } finally {
            $this->cleanupInternalStage($tableName);
        }
    }

    private function putIntoInternalStage(ExportConfig $exportConfig, string $tmpTableName): void
    {
        $putSql = $this->queryBuilder
            ->putFileQueryStatement($this->connection, $exportConfig->getTableFilePath(), $tmpTableName);

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

        if (!empty($databaseConfig->getPassword())) {
            $cliConfig[] = sprintf('password = "%s"', $databaseConfig->getPassword());
        }

        if ($databaseConfig->hasPrivateKey()) {
            $cliConfig[] = sprintf('private_key_path = "%s"', $databaseConfig->getPrivateKeyPath());
        }

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
