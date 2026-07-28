<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Hi\Kafka\Internal\KafkaExceptionFactory;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Socket;

/**
 * Swoole 协程感知的 Kafka 客户端。
 *
 * 与 `Hi\Kafka\Client`（C 扩展暴露、阻塞 IO）相对，本类：
 *
 * - 使用 `Swoole\Coroutine\Socket` 做 UDS 通信，所有 IO 走 Swoole reactor
 * - 用 `Channel` 实现协程感知的连接池，多协程并发不会互相阻塞
 * - 协议编解码复用扩展暴露的 `hi_kafka_*` 函数，**协议逻辑单源**
 * - 连接池仅服务短生命周期 RPC；Consumer 始终使用不入池的独占 stream connection
 *
 * 仅在 Swoole 协程上下文中使用。PHP-FPM / CLI / 非协程 Swoole 用 `Client`。
 *
 * 用法：
 *
 * ```php
 * use Swoole\Coroutine;
 * use Hi\Kafka\SwooleClient;
 *
 * Coroutine\run(function () {
 *     $client = new SwooleClient('/tmp/hi-kafka-v2.sock');
 *     $client->produceFnf('default', 'topic', 'k', 'v');
 *     $r = $client->produceSync('default', 'topic', 'k', 'v', timeoutMs: 5000);
 *     // $r => ['ok' => true, 'partition' => 0, 'offset' => 42]
 * });
 * ```
 */
final class SwooleClient implements ClientInterface
{
    private const SOCK_STREAM = 1;
    private const AF_UNIX = 1;

    private Channel $idleConns;
    private int $created = 0;
    private bool $workerEnsured = false;
    private int $errorFrameKind = 0;

    /**
     * 初始化 Swoole RPC 连接池，并验证当前扩展提供 driver 所需的协议函数。
     *
     * @param string $socket         Worker UDS 路径
     * @param int    $maxIdle        池上限。idle 满了归还时直接 close
     * @param float  $connectTimeout
     */
    public function __construct(
        private readonly string $socket = '/tmp/hi-kafka-v2.sock',
        private readonly int $maxIdle = 16,
        private readonly float $connectTimeout = 1.0,
    ) {
        $this->idleConns = new Channel($maxIdle);
        $this->assertExtension();
        $this->errorFrameKind = \hi_kafka_error_frame_kind();
    }

    /**
     * 优雅关闭 idle 连接池。框架容器 `#[Finalize]` 在 worker shutdown 时于协程
     * 上下文调用（经 `KafkaManager::finalize`）；也被 `__destruct` 兜底。
     *
     * 非协程上下文直接返回——Swoole `Channel->pop` 必须在协程内，否则抛
     * "API must be called in the coroutine" fatal。idle 连接随进程退出由 OS 回收。
     */
    public function close(): void
    {
        if (\Swoole\Coroutine::getCid() < 0) {
            return;
        }
        while (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->pop(0.001);
            if ($conn instanceof Socket) {
                $conn->close();
            }
        }
    }

    /**
     * 析构时尽力释放空闲连接；非协程上下文由 close() 安全跳过。
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
     * @param array<string,string>|null $headers     Kafka 消息头（traceparent / source 等）
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
        $timeoutSec = ($timeoutMs ?? 5000) / 1000.0;
        $conn = $this->acquire();

        try {
            $sent = $conn->sendAll($frame, $timeoutSec);
            if (false === $sent || $sent < \strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write'),
                );
            }
            // FNF 分层：读 worker 本地 enqueue ack。cluster 不存在 / 队列满等同步可知
            // 错误会以 Error 帧回来 → KafkaException；不等 broker delivery。
            $headerLen = \hi_kafka_header_len();
            $header = $conn->recvAll($headerLen, $timeoutSec);
            if (false === $header || \strlen($header) < $headerLen) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll fnf ack header failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }
            $parsed = \hi_kafka_parse_header($header);
            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $conn->recvAll($payloadLen, $timeoutSec)
                : '';
            if ($payloadLen > 0 && (
                false === $payload || \strlen($payload) < $payloadLen
            )) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll fnf ack payload failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }
            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
        } catch (KafkaException $ke) {
            throw $ke; // 连接已归还，业务错误不污染连接池
        } catch (\Throwable $e) {
            $conn->close();
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
        $timeoutSec = $timeoutMs / 1000.0;

        $conn = $this->acquire();

        try {
            $sent = $conn->sendAll($frame, $timeoutSec);
            if (false === $sent || $sent < \strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write'),
                );
            }

            $headerLen = \hi_kafka_header_len();
            $header = $conn->recvAll($headerLen, $timeoutSec);
            if (false === $header || \strlen($header) < $headerLen) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll header failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }
            $parsed = \hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                $conn->close();
                throw new \RuntimeException(
                    "cid mismatch: sent {$cid}, got {$parsed['cid']}",
                );
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $conn->recvAll($payloadLen, $timeoutSec)
                : '';
            if ($payloadLen > 0 && (
                false === $payload || \strlen($payload) < $payloadLen
            )) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll payload failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }

            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
            return \hi_kafka_decode_resp_frame($header . $payload);
        } catch (KafkaException $ke) {
            throw $ke;
        } catch (\Throwable $e) {
            $conn->close();
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
            new SwooleConsumerTransport,
            $cluster,
            $groupId,
            $topics,
            $config ?? [],
            connectTimeoutMs: (int) \ceil($this->connectTimeout * 1000),
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
            'idle' => $this->idleConns->length(),
            'created' => $this->created,
        ];
    }

    /**
     * 在可复用 RPC 连接上完成「写请求、读 header、校验 cid、读 payload、解码响应」。
     *
     * 协议或业务 Error 会在连接安全归还池后转换为 KafkaException；传输或帧错误会
     * 关闭连接，防止污染后续请求。
     *
     * @return array 结构同 hi_kafka_decode_control_resp 输出
     */
    private function roundTrip(int $cid, string $frame, ?int $timeoutMs = null): array
    {
        $timeoutMs ??= 5000;
        $timeoutSec = $timeoutMs / 1000.0;
        $conn = $this->acquire();

        try {
            $sent = $conn->sendAll($frame, $timeoutSec);
            if (false === $sent || $sent < \strlen($frame)) {
                $conn->close();
                throw new \RuntimeException(
                    'sendAll failed: ' . ($conn->errMsg ?: 'short write'),
                );
            }

            $headerLen = \hi_kafka_header_len();
            $header = $conn->recvAll($headerLen, $timeoutSec);
            if (false === $header || \strlen($header) < $headerLen) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll header failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }
            $parsed = \hi_kafka_parse_header($header);
            if ($parsed['cid'] !== $cid) {
                $conn->close();
                throw new \RuntimeException(
                    "cid mismatch: sent {$cid}, got {$parsed['cid']}",
                );
            }

            $payloadLen = $parsed['payload_len'];
            $payload = $payloadLen > 0
                ? $conn->recvAll($payloadLen, $timeoutSec)
                : '';
            if ($payloadLen > 0 && (
                false === $payload || \strlen($payload) < $payloadLen
            )) {
                $conn->close();
                throw new \RuntimeException(
                    'recvAll payload failed: ' . ($conn->errMsg ?: 'short read'),
                );
            }

            $this->release($conn);
            if ($parsed['kind'] === $this->errorFrameKind) {
                throw $this->makeKafka($header, $payload);
            }
            return \hi_kafka_decode_control_resp($header . $payload);
        } catch (KafkaException $ke) {
            throw $ke;
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
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
     * 从池中取得一条仍存活的 RPC 连接；没有可用空闲连接时新建并握手。
     */
    private function acquire(): Socket
    {
        // 池里有空闲就拿，但要做半死连接探测——worker 崩溃重启后池里的旧 fd 会挂到用时才炸。
        // Swoole 4.5+ 的 checkLiveness() 内部走 nonblocking peek，探测 peer 是否已关连接。
        // 老版本缺此方法 → 只能盲发盲收，与 checkLiveness 缺失前的行为一致（首次撞死一个连接才踢出）。
        while (! $this->idleConns->isEmpty()) {
            $conn = $this->idleConns->pop(0.001);
            if (! $conn instanceof Socket) {
                continue;
            }
            if (\method_exists($conn, 'checkLiveness') && ! $conn->checkLiveness()) {
                $conn->close();
                continue;
            }
            return $conn;
        }
        return $this->newConn();
    }

    /**
     * 将健康 RPC 连接归还池；池满或并发 push 失败时直接关闭，避免 fd 泄漏。
     */
    private function release(Socket $conn): void
    {
        if ($this->idleConns->isFull()) {
            $conn->close();
            return;
        }
        // push 可能因多协程并发 race 而超时返回 false（isFull 检查后到实际 push 期间池被塞满）。
        // 不接住会漏 fd —— 显式 close 兜底。
        if (! $this->idleConns->push($conn, 0.001)) {
            $conn->close();
        }
    }

    /**
     * 确保 worker 已启动，创建 UDS 连接并完成 HELLO 后计入本进程创建数。
     */
    private function newConn(): Socket
    {
        // 首次连接前确保 worker 已 fork 起来。
        // 走扩展的 hi_kafka_ensure_worker，重用其 flock + double-fork 互斥逻辑。
        if (! $this->workerEnsured) {
            \hi_kafka_ensure_worker($this->socket);
            $this->workerEnsured = true;
        }

        $conn = new Socket(self::AF_UNIX, self::SOCK_STREAM, 0);
        if (! $conn->connect($this->socket, 0, $this->connectTimeout)) {
            throw new \RuntimeException(
                "connect {$this->socket} failed: " . $conn->errMsg,
            );
        }

        // F: 协议 HELLO 握手——双端 PROTOCOL_MAJOR 不一致 worker 会关连接，
        // 这里同步发 HELLO + 读 14B RESP + 校验
        try {
            $this->handshake($conn);
        } catch (\Throwable $e) {
            $conn->close();
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
        $timeoutSec = 2.0;
        $sent = $conn->sendAll($frame, $timeoutSec);
        if (false === $sent || $sent < \strlen($frame)) {
            throw new \RuntimeException(
                'send HELLO failed: ' . ($conn->errMsg ?: 'short write'),
            );
        }
        $header = $conn->recvAll(\hi_kafka_header_len(), $timeoutSec);
        if (false === $header || \strlen($header) !== \hi_kafka_header_len()) {
            throw new \RuntimeException(
                'recv HELLO RESP failed: ' . ($conn->errMsg ?: 'short read'),
            );
        }
        $parsed = \hi_kafka_parse_header($header);
        $payload = $conn->recvAll($parsed['payload_len'], $timeoutSec);
        \hi_kafka_verify_hello_resp($header . $payload);
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
     * 在构造阶段验证 Swoole 运行时和所需 ext-kafka 协议函数，提前暴露版本错配。
     */
    private function assertExtension(): void
    {
        // 构造函数下一行就要调 hi_kafka_error_frame_kind()；缺任何一个都直接 fatal，
        // 这里显式列出全部构造期依赖的探针函数，让报错落到清晰的 RuntimeException 而不是 undefined function。
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
        if (! \extension_loaded('swoole')) {
            throw new \RuntimeException('swoole extension is required for SwooleClient');
        }
    }
}
