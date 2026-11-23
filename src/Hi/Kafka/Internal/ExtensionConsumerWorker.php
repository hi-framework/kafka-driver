<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * 通过 ext-kafka 启动 worker 并维护进程级集群注册重放缓存的适配器。
 *
 * @internal
 */
final class ExtensionConsumerWorker implements ConsumerWorkerInterface
{
    public function ensure(string $socket): void
    {
        \hi_kafka_ensure_worker($socket);
    }

    public function replay(string $socket): void
    {
        \hi_kafka_replay_clusters($socket);
    }

    public function rememberCluster(string $socket, string $cluster, array $config): void
    {
        \hi_kafka_remember_cluster($cluster, $config, $socket);
    }

    public function rememberOAuthToken(
        string $socket,
        string $cluster,
        string $token,
        int $lifetimeMs,
        string $principalName,
        array $extensions,
    ): void {
        \hi_kafka_remember_oauth_token($cluster, $token, $lifetimeMs, $principalName, $extensions, $socket);
    }
}
