<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

readonly class SnowflakeItemConfig extends ItemConfig
{
    /**
     * @param $config array{
     *     name: string,
     *     dbName: string,
     *     type: string,
     *     size?: string,
     *     nullable?: bool,
     *     default?: string,
     * }
     */
    public static function fromArray(array $config): self
    {
        return new self(
            $config['name'],
            $config['dbName'],
            $config['type'],
            $config['size'] ? (string) $config['size'] : null,
            $config['nullable'] ?? null,
            $config['default'] ?? null,
        );
    }
}
