<?php

declare(strict_types=1);

use Keboola\DbWriter\Snowflake\FunctionalTests\DatadirTest;
use PHPUnit\Framework\Assert;

return function (DatadirTest $test): void {
    Assert::assertSame([
        'full_name' => 'Full name',
        'glasses' => null,
        'id' => 'Person identifier',
    ], $test->getColumnComments('description'));
};
