<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\NodeDefinition;

use Keboola\DbWriterConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class SnowflakeDbNode extends DbNode
{
    public function init(NodeBuilder $nodeBuilder): void
    {
        parent::init($nodeBuilder);
        $this->addWarehouseNode($nodeBuilder);
        $this->addRunIdNode($nodeBuilder);
        $this->addPrivateKeyNode($nodeBuilder);
    }

    protected function addHostNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('host')->isRequired()->cannotBeEmpty();
    }

    protected function addDatabaseNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('database')->isRequired()->cannotBeEmpty();
    }

    protected function addSchemaNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('schema')->isRequired()->cannotBeEmpty();
    }

    protected function addWarehouseNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('warehouse');
    }

    protected function addRunIdNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('runId');
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        $this->beforeNormalization()->always(function (array $v) {
            if (isset($v['password'])) {
                $v['#password'] = $v['password'];
                unset($v['password']);
            }
            return $v;
        });

        $builder->scalarNode('#password');
    }

    protected function addPrivateKeyNode(NodeBuilder $builder): void
    {
        $this->beforeNormalization()->always(function (array $v) {
            if (isset($v['privateKey'])) {
                $v['#privateKey'] = $v['privateKey'];
                unset($v['privateKey']);
            }
            return $v;
        });

        $builder->scalarNode('#privateKey');
    }
}
