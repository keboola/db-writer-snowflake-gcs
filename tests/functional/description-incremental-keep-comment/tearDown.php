<?php

declare(strict_types=1);

use Keboola\DbWriter\Snowflake\FunctionalTests\DatadirTest;
use PHPUnit\Framework\Assert;

return function (DatadirTest $test): void {
    Assert::assertSame('Existing table comment', $test->getTableComment('description'));
    Assert::assertSame([
        'glasses' => null,
        'id' => 'Person identifier',
        'name' => 'Existing name comment',
    ], $test->getColumnComments('description'));
};
