<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Hi\Kafka\Internal\KafkaExceptionFactory;
use Swow\Socket;
use Swow\SocketException;

/**
 * Swow 协程感知的 Kafka 客户端。
 *
 * 与 `Hi\Kafka\Client`（C 扩展、阻塞 IO）对应，本类：
 *
 * - 用 `Swow\Socket` 做 UDS 通信，所有 IO 走 Swow 调度器
 * - 用 `SplQueue` 做协程感知连接池（Swow 协程协作式调度，无需线程安全队列）
 * - 协议编解码复用扩展暴露的 `hi_kafka_*` 全局函数，**协议逻辑单源**
 * - 连接池仅服务短生命周期 RPC；Consumer 始终使用不入池的独占 stream connection
 *
 * 仅在 Swow 协程上下文中使用。非协程或 Swoole 上下文用 `SwooleClient` / `Client`。
 *
 * 用法：
 *
 * ```php
 * use Swow\Coroutine;
 * use Hi\Kafka\SwowClient;
 *
 * Coroutine::run(function () {
 *     $client = new SwowClient('/tmp/hi-kafka.sock');
 *     $client->registerCluster('default', ['bootstrap.servers' => '127.0.0.1:9094']);
 *     $client->produceFnf('default', 'topic', 'k', 'v');
 *     $r = $client->produceSync('default', 'topic', 'k', 'v', timeoutMs: 5000);
 *     // $r => ['ok' => true, 'cid' => int, 'partition' => 0, 'offset' => 42]
 * });
 * ```
 */
final class SwowClient implements ClientInterface
{
    // 注：刻意不写 `private const TYPE_UNIX = Socket::TYPE_UNIX;` ——
    // 那会让 SwowClient 类本身被解析时即触发 Swow\Socket 加载。
    // 我们希望「类可被声明/autoload，运行时再检查 swow 扩展」，所以
    // Socket::TYPE_UNIX 留到 `newConn()` 里访问。

    /**
     * @var \SplQueue<Socket>
     */
    private \SplQueue $idleConns;
    private int $created = 0;
    private bool $workerEnsured = false;
    private int $errorFrameKind = 0;

    /**
     * 初始化 Swow RPC 连接池，并验证当前扩展提供 driver 所需的协议函数。
     *
     * @param string $socket           Worker UDS 路径
     * @param int    $maxIdle          池容量上限（多余的归还时直接 close）
     * @param int    $connectTimeoutMs 建链超时（毫秒）；-1 = 不超时
     */
    public function __construct(
        private readonly string $socket = '/tmp/hi-kafka-v2.sock',
        private readonly int $maxIdle = 16,
        private readonly int $connectTimeoutMs = 1000,
    ) {
        $this->idleConns = new \SplQueue;
        $this->assertExtension();
        $this->errorFrameKind = \hi_kafka_error_frame_kind();
    }

    /**
     * 优雅关闭 idle 连接池。框架容器 `#[Finalize]` 在 worker shutdown 时调用
     * （经 `KafkaManager::finalize`）；也被 `__destruct` 兜底。
     */
    public function close(): void
    {
        while (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->dequeue();
            if ($conn instanceof Socket) {
                try {
                    $conn->close();
                } catch (\Throwable) {
                    // GC 阶段忽略连接关闭错误
                }
            }
        }
    }

    /**
     * 析构时尽力释放所有空闲 RPC 连接，关闭异常由 close() 吞掉。
     */
    public function __destruct()
    {
        $this->close();
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
     *                                               —— **LSP 扩展**：追加可选参数，接口/扩展 Client 无此参数不影响
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
        $frame = \hi_kafka_encode_fnf_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs);
        $timeoutMs ??= 5000;
        $conn = $this->acquire();

        try {
            $this->sendAll($conn, $frame, $timeoutMs);
            // FNF 分层：读 worker 本地 enqueue ack。cluster 不存在 / 队列满等同步可知
            // 错误会以 Error 帧回来 → KafkaException；不等 broker delivery。
            $headerLen = \hi_kafka_header_len();
            $header = $this->receiveExactly($conn, $headerLen, $timeoutMs);
            $parsed = \hi_kafka_parse_header($header);
            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $this->receiveExactly($conn, $payloadLen, $timeoutMs)
                : '';
            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
        } catch (KafkaException $ke) {
            throw $ke; // 连接已归还，业务错误不污染连接池
        } catch (\Throwable $e) {
            $this->safeClose($conn);
            throw $e;
        }
    }

    /**
     * 生产消息并等待 broker delivery report，响应 cid 必须与请求一致。
     *
     * @param array<string,string>|null $headers     Kafka 消息头
     * @param int|null                  $partition   明确写入分区；null = librdkafka partitioner
     * @param int|null                  $timestampMs 消息时间戳（毫秒）
     * @param int|null                  $timeoutMs   单次 IPC IO 超时（毫秒），不是整次操作总预算
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
        $timeoutMs ??= 5000;
        $encoded = \hi_kafka_encode_req_frame($cluster, $topic, $key, $value, $headers, $partition, $timestampMs, $timeoutMs);
        $cid = $encoded['cid'];
        $frame = $encoded['frame'];

        $conn = $this->acquire();

        try {
            $this->sendAll($conn, $frame, $timeoutMs);

            $headerLen = \hi_kafka_header_len();
            $header = $this->receiveExactly($conn, $headerLen, $timeoutMs);
            $parsed = \hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                throw new \RuntimeException("cid mismatch: sent {$cid}, got {$parsed['cid']}");
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $this->receiveExactly($conn, $payloadLen, $timeoutMs)
                : '';

            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
            return \hi_kafka_decode_resp_frame($header . $payload);
        } catch (KafkaException $ke) {
            throw $ke;
        } catch (\Throwable $e) {
            $this->safeClose($conn);
            throw $e;
        }
    }

    /**
     * 通过普通 RPC 连接在 worker 中注册或覆盖一个 Kafka 集群。
     *
     * @param array<string,string> $config librdkafka 配置，必须包含 `bootstrap.servers`
     */
    public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void
    {
        $encoded = \hi_kafka_encode_register_cluster_frame($cluster, $config);
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("registerCluster failed: {$resp['message']}");
        }
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
            new SwowConsumerTransport,
            $cluster,
            $groupId,
            $topics,
            $config ?? [],
            connectTimeoutMs: $this->connectTimeoutMs,
        );
    }

    /**
     * 通过普通 RPC 连接向 worker 更新指定集群的 SASL/OAUTHBEARER token。
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
        $encoded = \hi_kafka_encode_set_oauth_token_frame(
            $cluster,
            $token,
            $lifetimeMs,
            $principalName,
            $extensions ?? [],
        );
        $resp = $this->roundTrip($encoded['cid'], $encoded['frame'], $timeoutMs);
        if (! $resp['ok']) {
            throw new \RuntimeException("setOAuthBearerToken failed: {$resp['message']}");
        }
    }

    /**
     * 返回当前进程内 RPC 连接池统计，供监控与排障使用。
     *
     * @return array{socket:string,max_idle:int,idle:int,created:int}
     */
    public function stats(): array
    {
        return [
            'socket' => $this->socket,
            'max_idle' => $this->maxIdle,
            'idle' => $this->idleConns->count(),
            'created' => $this->created,
        ];
    }

    /**
     * 显式确保 worker 已启动；同一客户端实例只调用一次扩展 ensure。
     *
     * 通常无需手动调用，因为首次新建 RPC 连接或 Consumer stream 时也会 ensure。
     */
    public function ensureWorker(): void
    {
        if (! $this->workerEnsured) {
            \hi_kafka_ensure_worker($this->socket);
            $this->workerEnsured = true;
        }
    }

    /**
     * 在可复用 RPC 连接上完成「写请求、读 header、校验 cid、读 payload、解码响应」。
     *
     * 协议或业务 Error 会在连接安全归还池后转换为 KafkaException；传输或帧错误会
     * 关闭连接，防止污染后续请求。
     *
     * @return array<string,mixed>
     */
    private function roundTrip(int $cid, string $frame, ?int $timeoutMs = null): array
    {
        $timeoutMs ??= 5000;
        $conn = $this->acquire();

        try {
            $this->sendAll($conn, $frame, $timeoutMs);

            $headerLen = \hi_kafka_header_len();
            $header = $this->receiveExactly($conn, $headerLen, $timeoutMs);
            $parsed = \hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                throw new \RuntimeException("cid mismatch: sent {$cid}, got {$parsed['cid']}");
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $this->receiveExactly($conn, $payloadLen, $timeoutMs)
                : '';

            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
            return \hi_kafka_decode_control_resp($header . $payload);
        } catch (KafkaException $ke) {
            throw $ke;
        } catch (\Throwable $e) {
            $this->safeClose($conn);
            throw $e;
        }
    }

    /**
     * 从池中取得一条仍可用的 RPC 连接；没有可用空闲连接时新建并握手。
     */
    private function acquire(): Socket
    {
        // Swow 协程协作式调度：单进程内 SplQueue 操作是原子的（没有抢占）。
        while (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->dequeue();
            // Swow Socket 提供 isAvailable() 探测连接活性
            if ($conn instanceof Socket && $conn->isAvailable()) {
                return $conn;
            }
            // 不活的连接直接丢弃
            $this->safeClose($conn instanceof Socket ? $conn : null);
        }
        return $this->newConn();
    }

    /**
     * 将健康 RPC 连接归还池；超过容量上限时直接关闭。
     */
    private function release(Socket $conn): void
    {
        if ($this->idleConns->count() >= $this->maxIdle) {
            $this->safeClose($conn);
            return;
        }
        $this->idleConns->enqueue($conn);
    }

    /**
     * 确保 worker 已启动，创建 UDS 连接并完成 HELLO 后计入本进程创建数。
     */
    private function newConn(): Socket
    {
        // 首次连接前确保 worker 已 fork 起来（扩展层 flock + double-fork 互斥）
        if (! $this->workerEnsured) {
            \hi_kafka_ensure_worker($this->socket);
            $this->workerEnsured = true;
        }

        $conn = new Socket(Socket::TYPE_UNIX);

        try {
            $conn->connect($this->socket, 0, $this->microseconds($this->connectTimeoutMs));
        } catch (SocketException $e) {
            $this->safeClose($conn);
            throw new \RuntimeException(
                "connect {$this->socket} failed: " . $e->getMessage(),
                $e->getCode(),
                $e,
            );
        }

        // F: 协议 HELLO 握手——双端 PROTOCOL_MAJOR 不一致 worker 会关连接
        try {
            $this->handshake($conn);
        } catch (\Throwable $e) {
            $this->safeClose($conn);
            throw new \RuntimeException(
                "handshake {$this->socket} failed: " . $e->getMessage(),
                0,
                $e,
            );
        }
        $this->created++;
        return $conn;
    }

    /**
     * 在新 RPC 连接上完成有界 HELLO 握手并校验 protocol major。
     */
    private function handshake(Socket $conn): void
    {
        $frame = \hi_kafka_encode_hello_frame();
        // 复用 connectTimeoutMs——connect 与 handshake 语义同属"建链耗时"，共用同一预算。
        // -1（不超时）时给 2s 保底，避免 handshake 卡住整个 driver。
        $timeoutMs = $this->connectTimeoutMs > 0 ? $this->connectTimeoutMs : 2000;
        $this->sendAll($conn, $frame, $timeoutMs);
        $header = $this->receiveExactly($conn, \hi_kafka_header_len(), $timeoutMs);
        $parsed = \hi_kafka_parse_header($header);
        $payload = $parsed['payload_len'] > 0
            ? $this->receiveExactly($conn, $parsed['payload_len'], $timeoutMs)
            : '';
        \hi_kafka_verify_hello_resp($header . $payload);
    }

    /**
     * 使用 Swow 的完整发送语义，在指定毫秒超时内写完全部字节。
     */
    private function sendAll(Socket $conn, string $data, int $timeoutMs): void
    {
        $conn->send($data, 0, -1, $this->microseconds($timeoutMs));
    }

    /**
     * 在指定毫秒超时内读取精确长度的字节串。
     */
    private function receiveExactly(Socket $conn, int $length, int $timeoutMs): string
    {
        return $conn->readString($length, $this->microseconds($timeoutMs));
    }

    /**
     * 把统一毫秒超时转换为 Swow 微秒；负数保持“不超时”语义。
     */
    private function microseconds(int $milliseconds): int
    {
        return $milliseconds < 0 ? -1 : $milliseconds * 1000;
    }

    /**
     * 尽力关闭可选连接，并吞掉清理路径上的关闭异常。
     */
    private function safeClose(?Socket $conn): void
    {
        if (null === $conn) {
            return;
        }

        try {
            $conn->close();
        } catch (\Throwable) {
            // 忽略连接关闭错误
        }
    }

    /**
     * 把 worker 回的 Error 帧解码成 KafkaException（不抛，由调用方 throw）。
     */
    private function makeKafka(string $header, string $payload): KafkaException
    {
        return KafkaExceptionFactory::fromDecoded(\hi_kafka_decode_error_frame($header . $payload));
    }

    /**
     * 在构造阶段验证 Swow 运行时和所需 ext-kafka 协议函数，提前暴露版本错配。
     */
    private function assertExtension(): void
    {
        // 构造函数下一行就要调 hi_kafka_error_frame_kind()；显式列出全部构造期依赖，
        // 让缺失时报错落到清晰的 RuntimeException 而不是 undefined function fatal。
        $required = [
            'hi_kafka_encode_fnf_frame',
            'hi_kafka_encode_consume_open_frame',
            'hi_kafka_decode_control_resp',
            'hi_kafka_error_frame_kind',
        ];
        foreach ($required as $fn) {
            if (! \function_exists($fn)) {
                throw new \RuntimeException(
                    "hi_kafka extension missing required function '{$fn}' (version skew?)",
                );
            }
        }
        if (! \extension_loaded('swow')) {
            throw new \RuntimeException('swow extension is required for SwowClient');
        }
    }
}
