<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SshConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SslConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;
use Keboola\SnowflakeDbAdapter\Exception\PrivateKeyIsNotValid;

readonly class SnowflakeDatabaseConfig extends DatabaseConfig
{
    public function __construct(
        ?string $host,
        ?string $port,
        string $database,
        private ?string $warehouse,
        private ?string $runId,
        string $user,
        ?string $password,
        private ?string $keyPair,
        ?string $schema,
        ?SshConfig $sshConfig,
        ?SslConfig $sslConfig,
    ) {
        parent::__construct($host, $port, $database, $user, $password, $schema, $sshConfig, $sslConfig);
    }

    public static function fromArray(array $config): self
    {
        $sshEnabled = $config['ssh']['enabled'] ?? false;
        $sslEnabled = $config['ssl']['enabled'] ?? false;
        $runId = $config['runId'] ?? getenv('KBC_RUNID');

        return new self(
            $config['host'],
            $config['port'] ?? null,
            $config['database'],
            $config['warehouse'] ?? null,
            $runId ?: null,
            $config['user'],
            $config['#password'] ?? '',
            $config['#keyPair'] ?? null,
            $config['schema'],
            $sshEnabled ? SshConfig::fromArray($config['ssh']) : null,
            $sslEnabled ? SslConfig::fromArray($config['ssl']) : null,
        );
    }

    public function hasWarehouse(): bool
    {
        return $this->warehouse !== null;
    }

    public function getWarehouse(): string
    {
        if ($this->warehouse === null) {
            throw new PropertyNotSetException('Property "warehouse" is not set.');
        }
        return $this->warehouse;
    }

    public function hasRunId(): bool
    {
        return $this->runId !== null;
    }

    public function getRunId(): string
    {
        if ($this->runId === null) {
            throw new PropertyNotSetException('Property "runId" is not set.');
        }
        return $this->runId;
    }

    public function hasKeyPair(): bool
    {
        return $this->keyPair !== null;
    }

    public function getKeyPair(): ?string
    {
        return $this->keyPair;
    }

    public function getKeyPairPath(): string
    {
        $privateKeyResource = openssl_pkey_get_private($this->getKeyPair() ?? '');
        if (!$privateKeyResource) {
            throw new PrivateKeyIsNotValid();
        }

        $pemPKCS8 = '';
        openssl_pkey_export($privateKeyResource, $pemPKCS8);

        $privateKeyPath = tempnam(sys_get_temp_dir(), 'snowflake_private_key_' . uniqid()) . '.p8';
        file_put_contents($privateKeyPath, $pemPKCS8);

        return $privateKeyPath;
    }
}
