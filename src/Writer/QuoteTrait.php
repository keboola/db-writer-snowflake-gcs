<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

trait QuoteTrait
{
    public function quote(string $str): string
    {
        return "'" . addslashes($str) . "'";
    }

    public function quoteIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }

    public function quoteManyIdentifiers(array $items, ?callable $mapper = null): array
    {
        return array_map(fn($item) => $this->quoteIdentifier($mapper ? $mapper($item) : $item), $items);
    }
}
