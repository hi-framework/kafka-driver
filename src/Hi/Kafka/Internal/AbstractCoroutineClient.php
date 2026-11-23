<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

use Hi\Kafka\ClientInterface;
use Hi\Kafka\ConsumerStreamInterface;
use Hi\Kafka\ConsumerTransportInterface;
use Hi\Kafka\CoroutineConsumerStream;

/**
 * Swoole / Swow 协程客户端的公共实现。
 *
 * 子类只提供 runtime adapter（{@see ConsumerTransportInterface}）和运行时检查；
 * 连接池、故障恢复、注册重放与 Consumer stream 创建全部在这里。
 *
 * @internal
 */
abstract class AbstractCoroutineClient implements ClientInterface
{
    private const DEFAULT_TIMEOUT_MS = 5000;

    private readonly CoroutineRpcClient $rpc;
    private readonly ConsumerProtocolInterface $protocol;
    private readonly ConsumerWorkerInterface $worker;

    protected function __construct(
        private readonly string $socket,
        int $maxIdle,
        private readonly int $connectTimeoutMs,
        ?ConsumerProtocolInterface $protocol = null,
        ?ConsumerWorkerInterface $worker = null,
    ) {
        $this->protocol = $protocol ?? new ExtensionConsumerProtocol;
        $this->worker = $worker ?? new ExtensionConsumerWorker;
        $this->rpc = new CoroutineRpcClient(
            $socket,
            fn (): ConsumerTransportInterface => $this->createTransport(),
            $maxIdle,
            $connectTimeoutMs,
            $this->protocol,
            $this->worker,
        );
    }

    /**
     * 为一条新连接创建 runtime adapter。
     */
    abstract protected function createTransport(): ConsumerTransportInterface;

    /**
     * 关闭空闲 RPC 连接。Consumer stream 由各自的对象拥有并关闭。
     */
    public function close(): void
    {
        $this->rpc->close();
    }

    public function __destruct()
    {
        try {
            $this->close();
        } catch (\Throwable) {
        }
    }

    /**
     * 把消息提交到 worker 本地生产队列，不等待 broker delivery report。
     *
     * 方法仍读取 worker 的本地 enqueue ack，因此集群不存在、队列已满等同步错误会
     * 立即抛出；成功不代表 broker 已持久化消息。
     *
     * @param array<string,string>|null $headers     Kafka 消息头
     * @param int|null                  $partition   明确写入分区；null = librdkafka partitioner（key hash）
     * @param int|null                  $timestampMs 消息时间戳（毫秒）；null = librdkafka 当前时间
     * @param int|null                  $timeoutMs   IPC 单次 IO 超时（毫秒），默认 5000
     */
    public function produceFnf(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
        ?int $timeoutMs = null,
    ): void {
        $this->rpc->call(function () use ($cluster, $topic, $key, $value, $headers, $partition, $timestampMs): ConsumerRequest {
            $frame = \hi_kafka_encode_fnf_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs);
            $header = $this->protocol->parseHeader(\substr($frame, 0, $this->protocol->headerLength()));
            return new ConsumerRequest($header['cid'], $frame);
        }, $timeoutMs ?? self::DEFAULT_TIMEOUT_MS);
    }

    /**
     * 生产消息并等待 broker delivery report。
     *
     * @param array<string,string>|null $headers     Kafka 消息头
     * @param int|null                  $partition   明确写入分区；null = librdkafka partitioner
     * @param int|null                  $timestampMs 消息时间戳（毫秒）
     * @param int|null                  $timeoutMs   worker 等待 delivery report 的上限（毫秒），默认 5000
     *
     * @return array<string,mixed> 解码后的 broker delivery 结果
     */
    public function produceSync(
        string $cluster,
        string $topic,
        string $key,
        string $value,
        ?array $headers = null,
        ?int $partition = null,
        ?int $timestampMs = null,
        ?int $timeoutMs = null,
    ): array {
        $timeoutMs ??= self::DEFAULT_TIMEOUT_MS;
        $raw = $this->rpc->call(function () use ($cluster, $topic, $key, $value, $headers, $partition, $timestampMs, $timeoutMs): ConsumerRequest {
            $encoded = \hi_kafka_encode_req_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs, $timeoutMs);
            return new ConsumerRequest($encoded['cid'], $encoded['frame']);
        }, $this->ipcTimeout($timeoutMs));

        return \hi_kafka_decode_resp_frame($raw);
    }

    /**
     * 在 worker 中注册或覆盖一个 Kafka 集群，并记录到进程级重放缓存。
     *
     * @param array<string,string> $config librdkafka 配置，必须包含 `bootstrap.servers`
     */
    public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void
    {
        $raw = $this->rpc->call(static function () use ($cluster, $config): ConsumerRequest {
            $encoded = \hi_kafka_encode_register_cluster_frame($cluster, $config);
            return new ConsumerRequest($encoded['cid'], $encoded['frame']);
        }, $timeoutMs ?? self::DEFAULT_TIMEOUT_MS);
        $this->assertControlOk($raw, 'registerCluster');
        $this->worker->rememberCluster($this->socket, $cluster, $config);
    }

    /**
     * 向 worker 更新指定集群的 SASL/OAUTHBEARER token，并记录到进程级重放缓存。
     *
     * @param array<string,string>|null $extensions OAUTHBEARER 扩展字段
     */
    public function setOAuthBearerToken(
        string $cluster,
        string $token,
        int $lifetimeMs,
        string $principalName,
        ?array $extensions = null,
        ?int $timeoutMs = null,
    ): void {
        $extensions ??= [];
        $raw = $this->rpc->call(static function () use ($cluster, $token, $lifetimeMs, $principalName, $extensions): ConsumerRequest {
            $encoded = \hi_kafka_encode_set_oauth_token_frame($cluster, $token, $lifetimeMs, $principalName, $extensions);
            return new ConsumerRequest($encoded['cid'], $encoded['frame']);
        }, $timeoutMs ?? self::DEFAULT_TIMEOUT_MS);
        $this->assertControlOk($raw, 'setOAuthBearerToken');
        $this->worker->rememberOAuthToken($this->socket, $cluster, $token, $lifetimeMs, $principalName, $extensions);
    }

    /**
     * 打开一条不进入 RPC 连接池的独占 protocol-v2 Consumer stream。
     *
     * @param list<string>              $topics
     * @param array<string,string>|null $config
     */
    public function consume(string $cluster, string $groupId, array $topics, ?array $config = null): ConsumerStreamInterface
    {
        return new CoroutineConsumerStream(
            $this->socket,
            $this->createTransport(),
            $cluster,
            $groupId,
            $topics,
            $config ?? [],
            connectTimeoutMs: $this->connectTimeoutMs,
            protocol: $this->protocol,
            worker: $this->worker,
        );
    }

    /**
     * 强制探测 worker，必要时启动它并重放本进程记录的集群。
     */
    public function ensureWorker(): void
    {
        $this->rpc->ensureWorker();
    }

    /**
     * 返回当前进程内 RPC 连接池统计，供监控与排障使用。
     *
     * @return array{socket:string,max_idle:int,idle:int,created:int}
     */
    public function stats(): array
    {
        return $this->rpc->stats();
    }

    /**
     * 构造期检查扩展协议函数与 runtime 扩展，提前暴露版本错配。
     */
    protected static function assertExtension(string $runtimeExtension): void
    {
        $required = [
            'hi_kafka_encode_fnf_frame',
            'hi_kafka_encode_consume_open_frame',
            'hi_kafka_decode_control_resp',
            'hi_kafka_remember_cluster',
            'hi_kafka_replay_clusters',
        ];
        foreach ($required as $fn) {
            if (! \function_exists($fn)) {
                throw new \RuntimeException(
                    "hi_kafka extension missing required function '{$fn}' (version skew?)",
                );
            }
        }
        if (! \extension_loaded($runtimeExtension)) {
            throw new \RuntimeException("{$runtimeExtension} extension is required for " . static::class);
        }
    }

    /**
     * worker 在 `timeoutMs` 到期时返回结构化 TIMEOUT；IPC 读超时额外留出余量，
     * 保证调用方拿到的是 worker 的明确结果，而不是本地读超时。
     */
    private function ipcTimeout(int $timeoutMs): int
    {
        return $timeoutMs + \min(1000, \max(100, \intdiv($timeoutMs, 10)));
    }

    private function assertControlOk(string $raw, string $operation): void
    {
        $resp = \hi_kafka_decode_control_resp($raw);
        if (! $resp['ok']) {
            throw new \RuntimeException("{$operation} failed: {$resp['message']}");
        }
    }
}
