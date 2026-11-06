<?php

declare(strict_types=1);

use Keboola\DbWriter\Snowflake\FunctionalTests\DatadirTest;
use PHPUnit\Framework\Assert;

return function (DatadirTest $test): void {
    Assert::assertSame('People and their glasses', $test->getTableComment('description'));
    Assert::assertSame([
        'glasses' => null,
        'id' => 'Person identifier',
        'name' => 'Full name',
    ], $test->getColumnComments('description'));
};
