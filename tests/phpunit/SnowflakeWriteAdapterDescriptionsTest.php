<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Writer\SnowflakeConnection;
use Keboola\DbWriter\Writer\SnowflakeQueryBuilder;
use Keboola\DbWriter\Writer\SnowflakeWriteAdapter;
use Keboola\DbWriterAdapter\DescriptionWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class SnowflakeWriteAdapterDescriptionsTest extends TestCase
{
    /** @var string[] */
    private array $executedQueries = [];

    private SnowflakeWriteAdapter $adapter;

    protected function setUp(): void
    {
        $this->executedQueries = [];

        $connection = $this->createMock(SnowflakeConnection::class);
        $connection->method('exec')->willReturnCallback(function (string $sql): void {
            $this->executedQueries[] = $sql;
        });

        $this->adapter = new SnowflakeWriteAdapter(
            $connection,
            $this->createMock(SnowflakeQueryBuilder::class),
            new TestLogger(),
        );
    }

    public function testImplementsDescriptionWriteAdapter(): void
    {
        self::assertInstanceOf(DescriptionWriteAdapter::class, $this->adapter);
    }

    public function testTableAndColumnDescriptions(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            'Table description',
            ['id' => 'Identifier', 'name' => 'Customer name'],
        ));

        self::assertSame([
            'COMMENT ON TABLE "simple" IS \'Table description\'',
            'COMMENT ON COLUMN "simple"."id" IS \'Identifier\'',
            'COMMENT ON COLUMN "simple"."name" IS \'Customer name\'',
        ], $this->executedQueries);
    }

    public function testColumnDescriptionsOnly(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['id' => 'Identifier']));

        self::assertSame([
            'COMMENT ON COLUMN "simple"."id" IS \'Identifier\'',
        ], $this->executedQueries);
    }

    public function testEmptyTableDescriptionIsSkipped(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions('', ['id' => 'Identifier']));

        self::assertSame([
            'COMMENT ON COLUMN "simple"."id" IS \'Identifier\'',
        ], $this->executedQueries);
    }

    public function testNumericColumnNameIsQuotedAsString(): void
    {
        // PHP stores the key as int 123, which is what TableDescriptions really hands to implementers.
        /** @var array<string, string> $columns */
        $columns = ['123' => 'Numeric column'];
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, $columns));

        self::assertSame([
            'COMMENT ON COLUMN "simple"."123" IS \'Numeric column\'',
        ], $this->executedQueries);
    }

    public function testQuotesAreEscaped(): void
    {
        $this->adapter->applyDescriptions('my "table"', new TableDescriptions(
            'It\'s a "table" with \\ backslash',
            ['col "x"' => 'Column\'s value'],
        ));

        self::assertSame([
            'COMMENT ON TABLE "my ""table""" IS \'It\\\'s a \\"table\\" with \\\\ backslash\'',
            'COMMENT ON COLUMN "my ""table"""."col ""x""" IS \'Column\\\'s value\'',
        ], $this->executedQueries);
    }
}
