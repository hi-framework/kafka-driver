<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * @internal worker 生命周期与集群注册重放边界
 */
interface ConsumerWorkerInterface
{
    /**
     * 强制探测指定 UDS 路径对应的 worker，必要时启动它并重放本进程记录的集群。
     */
    public function ensure(string $socket): void;

    /**
     * 确保 worker 存活，并无条件重放本进程记录的全部集群注册与 OAuth token。
     */
    public function replay(string $socket): void;

    /**
     * 记录一次已成功完成的集群注册，worker 重启后由扩展重放。
     *
     * @param array<string,string> $config
     */
    public function rememberCluster(string $socket, string $cluster, array $config): void;

    /**
     * 记录一次已成功推送的 OAuth token，worker 重启后由扩展重放。
     *
     * @param array<string,string> $extensions
     */
    public function rememberOAuthToken(
        string $socket,
        string $cluster,
        string $token,
        int $lifetimeMs,
        string $principalName,
        array $extensions,
    ): void;
}
