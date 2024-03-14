<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;
use Keboola\DbWriter\Configuration\NodeDefinition\SnowflakeDbNode;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\DbWriterConfig\Configuration\ConfigDefinition;
use Keboola\DbWriterConfig\Configuration\ConfigRowDefinition;
use Keboola\DbWriterConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class SnowflakeApplication extends Application
{
    protected string $writerName = 'Snowflake';

    protected function run(): void
    {
        $parameters = $this->getConfig()->getParameters();
        $parameters = $this->validateTableItems($parameters);

        $writerFactory = new WriterFactory($this->getConfig());
        /** @var Snowflake $writer */
        $writer = $writerFactory->create($this->getLogger(), $this->createDatabaseConfig($parameters['db']));
        $writer->write($this->createExportConfig($parameters));
    }

    protected function loadConfig(): void
    {
        $configClass = $this->getConfigClass();
        $configDefinitionClass = $this->getConfigDefinitionClass();

        if (in_array($configDefinitionClass, [ConfigRowDefinition::class, ConfigDefinition::class])) {
            $definition = new $configDefinitionClass(
                dbNode: (new SnowflakeDbNode())->ignoreExtraKeys(),
                tableNodesDecorator: new TableNodesDecorator(),
            );
        } else {
            $definition = new $configDefinitionClass(dbNode: new SnowflakeDbNode());
        }

        try {
            /** @var BaseConfig $config */
            $config = new $configClass(
                $this->getRawConfig(),
                $definition,
            );
            $this->config = $config;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function createDatabaseConfig(array $dbParams): SnowflakeDatabaseConfig
    {
        return SnowflakeDatabaseConfig::fromArray($dbParams);
    }
}
