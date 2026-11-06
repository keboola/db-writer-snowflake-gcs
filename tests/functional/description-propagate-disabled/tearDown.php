<?php

declare(strict_types=1);

use Keboola\DbWriter\Snowflake\FunctionalTests\DatadirTest;
use PHPUnit\Framework\Assert;

return function (DatadirTest $test): void {
    Assert::assertNull($test->getTableComment('description'));
    Assert::assertSame([
        'glasses' => null,
        'id' => null,
        'name' => null,
    ], $test->getColumnComments('description'));
};
