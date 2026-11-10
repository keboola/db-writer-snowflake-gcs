<?php

declare(strict_types=1);

use Keboola\DbWriter\Snowflake\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->connection->exec('CREATE TABLE "description" (
        "id" int,
        "name" VARCHAR(255) COMMENT \'Existing name comment\',
        "glasses" VARCHAR(255)
    ) COMMENT = \'Existing table comment\'');
    $test->orderResults = 'id';
};
