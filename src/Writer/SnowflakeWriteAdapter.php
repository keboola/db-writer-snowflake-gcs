<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Strategy\AbsWriteStrategy;
use Keboola\DbWriter\Writer\Strategy\S3WriteStrategy;
use Keboola\DbWriter\Writer\Strategy\WriteStrategy;
use Keboola\DbWriterAdapter\ODBC\OdbcWriteAdapter;
use Keboola\DbWriterAdapter\Query\QueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
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
        LoggerInterface $logger
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
//        $this->logger->info(sprintf('Writing data to table "%s"', $tableName));

        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $exportConfig->getDatabaseConfig();

        $this->snowSqlConfig = $this->createSnowSqlConfig($databaseConfig);

//        $stageName = $this->generateStageName($databaseConfig->hasRunId() ? $databaseConfig->getRunId() : '');

//        $this->logger->info(sprintf('Dropping stage "%s"', $stageName));
//        $this->connection->exec($this->queryBuilder->dropStageStatement($this->connection, $stageName));

        // Copy into table stage
        $this->logger->info(sprintf('Copying data to table stage "%s"', $tableName));
        $this->cleanupTableStage($tableName);
        $this->putIntoTableStage($tableName, $exportConfig);

        exit;
//        var_dump($exportConfig->getTableFilePath()); exit;
//        var_dump($exportConfig->getItems()); exit;

//        $writeStrategy = $this->getTableWriteStrategy($exportConfig->getTableFilePath());

        $this->logger->info(sprintf('Creating stage "%s"', $stageName));
        $this->connection->exec($writeStrategy->generateCreateStageCommand($stageName));

//        $tableNameWithSchema = sprintf(
//            '%s.%s',
//            $this->connection->quoteIdentifier($databaseConfig->getSchema()),
//            $this->connection->quoteIdentifier($tableName),
//        );
        try {
            $items = array_filter(
                $exportConfig->getItems(),
                fn(ItemConfig $item) => strtolower($item->getType()) !== 'ignore',
            );
            $commands = $writeStrategy->generateCopyCommands(
                tableName: $tableNameWithSchema,
                stageName: $stageName,
                items: $items,
            );
            foreach ($commands as $command) {
                $this->connection->exec($command);
            }
        } finally {
            $this->connection->exec($this->queryBuilder->dropStageStatement($this->connection, $stageName));
        }
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->connection->exec($sql);
    }

    private function putIntoTableStage(string $tmpTableName, ExportConfig $exportConfig): void
    {
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $exportConfig->getDatabaseConfig();

        $sql = [];
        if ($databaseConfig->hasWarehouse()) {
            $sql[] = sprintf(
                'USE WAREHOUSE %s;',
                $this->quoteIdentifier($databaseConfig->getWarehouse()),
            );
        }

        $sql[] = sprintf(
            'USE DATABASE %s;',
            $this->quoteIdentifier($databaseConfig->getDatabase()),
        );

        if ($databaseConfig->hasSchema()) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                $this->quoteIdentifier($databaseConfig->getDatabase()),
                $this->quoteIdentifier($databaseConfig->getSchema()),
            );
        }

        $sql[] = sprintf(
            'PUT file://%s @~/%s;',
            $exportConfig->getTableFilePath(),
            $tmpTableName,
        );

        $snowSql = $this->tempDir->createTmpFile('snowsql.sql');
        file_put_contents($snowSql->getPathname(), implode("\n", $sql));

        $this->logger->debug(trim(implode("\n", $sql)));

        $this->execSnowSql($snowSql);
    }

    private function execSnowSql(string|SplFileInfo $sql): Process
    {
        $command = sprintf(
            'snowsql --config %s -c writer %s',
            $this->snowSqlConfig,
            is_string($sql) ? sprintf('-q "%s"', $sql) : sprintf('-f %s', $sql)
        );

        $this->logger->debug(trim($command));

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
//            throw new Exception(sprintf(
//                'File download error occurred processing [%s]',
//                $exportConfig->hasTable() ? $exportConfig->getTable()->getName() : $exportConfig->getOutputTable(),
//            ));
        }

        return $process;
    }

    private function generateCopyCommand(string $stageTmpPath, string $query): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(','));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote('"'));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $this->quote('\\\\'));
        $csvOptions[] = sprintf('COMPRESSION = %s', $this->quote('GZIP'));
        $csvOptions[] = 'NULL_IF=()';

        return sprintf(
            '
            COPY INTO @~/%s/part
            FROM (%s)
            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = false
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ',
            $stageTmpPath,
            rtrim(trim($query), ';'),
            implode(' ', $csvOptions),
        );
    }

    //    public function upsert(ExportConfig $exportConfig, string $stageTableName): void
//    {
//        $this->logger->info(sprintf('Upserting data to table "%s"', $exportConfig->getDbName()));
//        if ($exportConfig->hasPrimaryKey()) {
//            $this->addPrimaryKeyIfMissing($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
//            $this->checkPrimaryKey($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
//        }
//
//        parent::upsert($exportConfig, $stageTableName);
//    }

    public function swapTable(SnowflakeConnection $connection, string $tableName, string $stagingTableName): void
    {
        // ToDo: do via snowsql
//        $this->logger->info(sprintf('Swapping table "%s" with "%s"', $stagingTableName, $tableName));
//        $connection->exec(sprintf(
//            'ALTER TABLE %s SWAP WITH %s',
//            $this->connection->quoteIdentifier($stagingTableName),
//            $this->connection->quoteIdentifier($tableName),
//        ));
    }

//    /**
//     * @return array{Field: string, Type: string}[]
//     */
//    public function getTableInfo(string $tableName): array
//    {
//        /** @var array{name: string, type: string}[] $res */
//        $res = $this->connection->fetchAll(
//            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
//        );
//
//        return array_map(fn(array $item) => [
//            'Field' => (string) $item['name'],
//            'Type' => (string) $item['type'],
//        ], $res);
//    }

    public function validateTable(string $tableName, array $items): void
    {
        // turn off validation
    }

//    public function getPrimaryKeys(string $tableName): array
//    {
//        $sqlPrimaryKeysInDb = $this->connection->fetchAll(
//            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
//        );
//        return array_filter($sqlPrimaryKeysInDb, fn($v) => $v['primary key'] === 'Y');
//    }

//    private function getTableWriteStrategy(string $getTableFilePath): WriteStrategy
//    {
//        /**
//         * @var array{s3?: array, abs?: array} $manifest
//         */
//        $manifest = json_decode(
//            (string) file_get_contents($getTableFilePath . '.manifest'),
//            true,
//        );
//
//        if (isset($manifest[WriteStrategy::FILE_STORAGE_S3])) {
//            $this->logger->info('Using S3 write strategy');
//            return new S3WriteStrategy($manifest[WriteStrategy::FILE_STORAGE_S3]);
//        }
//        if (isset($manifest[WriteStrategy::FILE_STORAGE_ABS])) {
//            $this->logger->info('Using ABS write strategy');
//            return new AbsWriteStrategy($manifest[WriteStrategy::FILE_STORAGE_ABS]);
//        }
//        throw new UserException('Unknown input adapter');
//    }

//    private function generateStageName(string $runId): string
//    {
//        $stageName = sprintf(
//            'db-writer-%s',
//            str_replace('.', '-', $runId),
//        );
//
//        return rtrim(mb_substr($stageName, 0, 255), '-');
//    }

//    private function addPrimaryKeyIfMissing(array $primaryKeys, string $tableName): void
//    {
//        $primaryKeysInDb = $this->getPrimaryKeys($tableName);
//
//        if (!empty($primaryKeysInDb)) {
//            return;
//        }
//
//        $this->connection->exec(
//            $this->queryBuilder->addPrimaryKeyQueryStatement($this->connection, $tableName, $primaryKeys),
//        );
//    }

//    private function checkPrimaryKey(array $primaryKeys, string $tableName): void
//    {
//        $primaryKeysInDb = $this->getPrimaryKeys($tableName);
//        $primaryKeysInDb = array_map(fn(array $item) => $item['name'], $primaryKeysInDb);
//
//        sort($primaryKeysInDb);
//        sort($primaryKeys);
//
//        if ($primaryKeysInDb !== $primaryKeys) {
//            throw new UserException(sprintf(
//                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
//                . 'Keys in configuration: %s' . PHP_EOL
//                . 'Keys in DB table: %s',
//                implode(',', $primaryKeys),
//                implode(',', $primaryKeysInDb),
//            ));
//        }
//    }

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
