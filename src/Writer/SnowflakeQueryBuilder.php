<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class SnowflakeQueryBuilder extends DefaultQueryBuilder
{
    private const TYPES_WITH_SIZE = [
        'number', 'decimal', 'numeric',
        'char', 'character', 'varchar', 'string', 'text', 'binary',
    ];

    public function __construct(readonly private SnowflakeDatabaseConfig $databaseConfig)
    {
    }

    public function createQueryStatement(
        Connection $connection,
        string $tableName,
        bool $isTempTable,
        array $items,
        ?array $primaryKeys = null,
    ): string {
        $itemSqlDefinition = $this->buildItemsSqlDefinition($connection, $items, $primaryKeys);

        return sprintf(
            'CREATE %sTABLE%s %s (%s)',
            $isTempTable ? 'TEMPORARY ' : '',
            !$isTempTable ? ' IF NOT EXISTS' : '',
            $connection->quoteIdentifier($tableName),
            $itemSqlDefinition,
        );
    }

    public function tableExistsQueryStatement(Connection $connection, string $tableName): string
    {
        return sprintf(
            '
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_CATALOG = %s
            ',
            $connection->quote($tableName),
            $connection->quote($this->databaseConfig->getSchema()),
            $connection->quote($this->databaseConfig->getDatabase()),
        );
    }

    public function addPrimaryKeyQueryStatement(Connection $connection, string $tableName, array $primaryKeys): string
    {
        return sprintf(
            'ALTER TABLE %s.%s ADD %s',
            $connection->quoteIdentifier($this->databaseConfig->getSchema()),
            $connection->quoteIdentifier($tableName),
            $this->buildPrimaryKeysSqlDefinition($connection, $primaryKeys),
        );
    }

    public function putFileQueryStatement(Connection $connection, string $tableFilePath, string $tmpTableName): string
    {
        $warehouse = $this->databaseConfig->hasWarehouse() ? $this->databaseConfig->getWarehouse() : null;
        $database = $this->databaseConfig->getDatabase();
        $schema = $this->databaseConfig->hasSchema() ? $this->databaseConfig->getSchema() : null;

        $sql = [];
        if ($warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', $connection->quoteIdentifier($warehouse));
        }

        $sql[] = sprintf('USE DATABASE %s;', $connection->quoteIdentifier($database));

        if ($schema) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                $connection->quoteIdentifier($database),
                $connection->quoteIdentifier($schema),
            );
        }

        $sql[] = sprintf(
            'PUT file://%s @~/%s;',
            $tableFilePath,
            $tmpTableName,
        );

        return trim(implode("\n", $sql));
    }

    public function copyIntoTableQueryStatement(Connection $connection, string $tmpTableName, array $items): string
    {
        $csvOptions = [
            'SKIP_HEADER = 1',
            sprintf('FIELD_DELIMITER = %s', $connection->quote(',')),
            sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $connection->quote('"')),
            sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $connection->quote('\\')),
            sprintf('COMPRESSION = %s', $connection->quote('GZIP')),
            'NULL_IF = (\'\')',
        ];

        $tmpTableNameWithSchema = sprintf(
            '%s.%s',
            $connection->quoteIdentifier($this->databaseConfig->getSchema()),
            $connection->quoteIdentifier($tmpTableName),
        );

        $columns = array_map(fn(ItemConfig $column) => $connection->quoteIdentifier($column->getDbName()), $items);

        return sprintf(
            '
            COPY INTO %s(%s)
            FROM @~/%s
            FILE_FORMAT = (TYPE=CSV %s)
            ;
            ',
            $tmpTableNameWithSchema,
            implode(', ', $columns),
            $tmpTableName,
            implode(' ', $csvOptions),
        );
    }

    public function upsertUpdateRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $sourceTable = $this->getTableNameWithSchema($connection, $stageTableName);
        $targetTable = $this->getTableNameWithSchema($connection, $exportConfig->getDbName());

        $columns = array_map(function ($item) {
            return $item->getDbName();
        }, $exportConfig->getItems());

        // update data
        $joinClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $targetTable,
            $connection->quoteIdentifier($item),
            $sourceTable,
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        $valuesClauseArr = array_map(fn($item) => sprintf(
            '%s = %s.%s',
            $connection->quoteIdentifier($item),
            $sourceTable,
            $connection->quoteIdentifier($item),
        ), $columns);
        $valuesClause = implode(',', $valuesClauseArr);

        return sprintf(
            'UPDATE %s SET %s FROM %s WHERE %s;',
            $targetTable,
            $valuesClause,
            $sourceTable,
            $joinClause,
        );
    }

    public function upsertDeleteRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $sourceTable = $this->getTableNameWithSchema($connection, $stageTableName);
        $targetTable = $this->getTableNameWithSchema($connection, $exportConfig->getDbName());

        $joinClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $targetTable,
            $connection->quoteIdentifier($item),
            $sourceTable,
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        return sprintf(
            'DELETE FROM %s USING %s WHERE %s',
            $sourceTable,
            $targetTable,
            $joinClause,
        );
    }

    public function tableInfoQueryStatement(Connection $connection, string $dbName): string
    {
        return sprintf(
            'DESCRIBE TABLE %s.%s',
            $connection->quoteIdentifier($this->databaseConfig->getSchema()),
            $connection->quoteIdentifier($dbName),
        );
    }

    private function buildItemsSqlDefinition(Connection $connection, array $items, ?array $primaryKeys = []): string
    {
        $sqlItems = [];

        /** @var ItemConfig $item */
        foreach ($items as $item) {
            if (strtolower($item->getType()) === 'ignore') {
                continue;
            }
            $sqlItems[] = sprintf(
                '%s %s%s %s %s',
                $connection->quoteIdentifier($item->getDbName()),
                strtoupper($item->getType()),
                $item->hasSize() && in_array($item->getType(), self::TYPES_WITH_SIZE) ?
                    sprintf('(%s)', $item->getSize()) :
                    '',
                $item->getNullable() ? 'NULL' : 'NOT NULL',
                $item->hasDefault() && $item->getType() !== 'TEXT' ?
                    sprintf(
                        'DEFAULT CAST(%s AS %s)',
                        $connection->quote($item->getDefault()),
                        $item->getType(),
                    ) :
                    '',
            );
        }

        // add Primary keys
        if ($primaryKeys) {
            $sqlItems[] = $this->buildPrimaryKeysSqlDefinition($connection, $primaryKeys);
        }
        return implode(', ', $sqlItems);
    }

    private function buildPrimaryKeysSqlDefinition(Connection $connection, array $primaryKeys): string
    {
        $quotedPK = array_map(fn($primaryColumn) => $connection->quoteIdentifier($primaryColumn), $primaryKeys);
        return sprintf(
            'PRIMARY KEY(%s)',
            implode(', ', $quotedPK),
        );
    }

    private function getTableNameWithSchema(Connection $connection, string $tableName): string
    {
        return sprintf(
            '%s.%s',
            $connection->quoteIdentifier($this->databaseConfig->getSchema()),
            $connection->quoteIdentifier($tableName),
        );
    }
}
