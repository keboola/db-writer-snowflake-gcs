<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\WriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * @property-read SnowflakeWriteAdapter $adapter
 */
class Snowflake extends BaseWriter
{
    /** @var SnowflakeConnection $connection */
    protected Connection $connection;

    private SnowflakeDatabaseConfig $databaseConfig;

    /**
     * @param SnowflakeDatabaseConfig $databaseConfig
     */
    public function __construct(DatabaseConfig $databaseConfig, LoggerInterface $logger)
    {
        $this->databaseConfig = $databaseConfig;
        parent::__construct($this->databaseConfig, $logger);
    }

    protected function writeFull(ExportConfig $exportConfig): void
    {
        $stageTableName = $this->adapter->generateTmpName($exportConfig->getDbName());

        $this->adapter->create(
            $stageTableName,
            false,
            $exportConfig->getItems(),
            $exportConfig->hasPrimaryKey() ? $exportConfig->getPrimaryKey() : null,
        );

        try {
            $this->adapter->create(
                $exportConfig->getDbName(),
                false,
                $exportConfig->getItems(),
                $exportConfig->hasPrimaryKey() ? $exportConfig->getPrimaryKey() : null,
            );

            $this->adapter->writeData($stageTableName, $exportConfig);
            $this->adapter->swapTable($this->connection, $exportConfig->getDbName(), $stageTableName);
        } finally {
            $this->adapter->drop($stageTableName);
        }
    }

    /**
     * @param SnowflakeDatabaseConfig $databaseConfig
     * @return SnowflakeConnection
     */
    protected function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        $connectionFactory = new SnowflakeConnectionFactory();
        $connection = $connectionFactory->create($databaseConfig, $this->logger);

        $warehouse = $databaseConfig->hasWarehouse() ? $databaseConfig->getWarehouse() : null;
        $this->validateAndSetWarehouse($connection, $warehouse);
        $this->validateAndSetSchema($connection, $databaseConfig->getSchema());

        return $connection;
    }

    protected function createWriteAdapter(): WriteAdapter
    {
        return new SnowflakeWriteAdapter(
            $this->connection,
            new SnowflakeQueryBuilder($this->databaseConfig),
            $this->logger,
        );
    }

    public function getCurrentUser(SnowflakeConnection $connection): string
    {
        /**
         * @var array{CURRENT_USER: string}[] $currentUser
         */
        $currentUser = $connection->fetchAll('SELECT CURRENT_USER;');
        return $currentUser[0]['CURRENT_USER'];
    }

    private function validateAndSetWarehouse(SnowflakeConnection $connection, ?string $warehouse): void
    {
        $this->logger->info(sprintf('Validating warehouse "%s"', $warehouse));
        if ($warehouse === null) {
            $warehouse = $this->getUserDefaultWarehouse($connection);
        }

        if ($warehouse === null) {
            throw new UserException(
                'Snowflake user has any "DEFAULT_WAREHOUSE" specified. Set "warehouse" parameter.',
            );
        }

        try {
            $connection->exec(sprintf('USE WAREHOUSE %s;', $connection->quoteIdentifier($warehouse)));
        } catch (Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    private function getUserDefaultWarehouse(SnowflakeConnection $connection): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            $connection->quoteIdentifier($this->getCurrentUser($connection)),
        );

        $config = $connection->fetchAll($sql);

        /**
         * @var array{'value': string}[] $defaultWarehouse
         */
        $defaultWarehouse = array_values(
            array_filter($config, fn ($item) => $item['property'] === 'DEFAULT_WAREHOUSE'),
        );

        if (count($defaultWarehouse) !== 1) {
            return null;
        }

        return $defaultWarehouse[0]['value'] === 'null' ? null : $defaultWarehouse[0]['value'];
    }

    private function validateAndSetSchema(SnowflakeConnection $connection, string $schema): void
    {
        $this->logger->info(sprintf('Validating schema "%s"', $schema));
        try {
            $connection->exec(sprintf(
                'USE SCHEMA %s;',
                $connection->quoteIdentifier($schema),
            ));
        } catch (Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid schema "%s" specified', $schema));
            } else {
                throw $e;
            }
        }
    }
}
