<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Hi\Kafka\Internal\ConsumerCredit;
use Hi\Kafka\Internal\ConsumerFrameReader;
use Hi\Kafka\Internal\ConsumerProtocolInterface;
use Hi\Kafka\Internal\ConsumerStreamState;
use Hi\Kafka\Internal\ConsumerWorkerInterface;
use Hi\Kafka\Internal\ConsumerWireFrame;
use Hi\Kafka\Internal\ExtensionConsumerProtocol;
use Hi\Kafka\Internal\ExtensionConsumerWorker;
use Hi\Kafka\Internal\KafkaExceptionFactory;

/**
 * Swoole 与 Swow 共用的 protocol-v2 消费编排器。
 *
 * 审阅导航：
 * - ConsumerStreamState 维护身份、信用额度与消息确认的全部不变量；
 * - ConsumerFrameReader 负责增量组帧以及仅属于当前连接的缓冲字节；
 * - ConsumerProtocolInterface 是代码对扩展协议编解码器的唯一依赖边界；
 * - 本类只负责组织 IO、控制屏障与连接恢复的执行顺序。
 */
final class CoroutineConsumerStream implements ConsumerStreamInterface
{
    private const MESSAGE_CREDIT = 1;
    private const CONTROL_TIMEOUT_MS = 5000;
    private const CLOSE_TIMEOUT_MS = 1000;
    private const HELLO_TIMEOUT_MS = 2000;
    private const ERROR_SUBSCRIPTION_NOT_FOUND = 6;
    private const ERROR_STALE_EPOCH = 7;
    private readonly ConsumerProtocolInterface $protocol;
    private readonly ConsumerWorkerInterface $worker;
    private readonly ConsumerFrameReader $frames;
    private readonly ConsumerStreamState $state;
    private int $negotiatedByteCredit;

    /**
     * 启动或确认 worker、建立独占连接并立即创建新的 subscription。
     *
     * protocol 与 worker 参数用于注入无扩展测试替身；生产环境默认使用 ext-kafka。
     * 构造任一步骤失败都会关闭 transport，避免遗留半初始化连接。
     *
     * @param list<string>         $topics
     * @param array<string,string> $config
     */
    public function __construct(
        private readonly string $socket,
        private readonly ConsumerTransportInterface $transport,
        private readonly string $cluster,
        private readonly string $groupId,
        private readonly array $topics,
        private readonly array $config = [],
        private readonly int $byteCredit = 16777216,
        private readonly int $connectTimeoutMs = 1000,
        ?ConsumerProtocolInterface $protocol = null,
        ?ConsumerWorkerInterface $worker = null,
    ) {
        if ($byteCredit <= 0 || $connectTimeoutMs < 0) {
            throw new \InvalidArgumentException('byte credit must be positive and connect timeout must not be negative');
        }

        $this->protocol = $protocol ?? new ExtensionConsumerProtocol;
        $this->worker = $worker ?? new ExtensionConsumerWorker;
        $this->frames = new ConsumerFrameReader($transport, $this->protocol);
        $this->state = new ConsumerStreamState;

        try {
            $this->worker->ensure($socket);
            $this->connectAndHello();
            $this->openFresh();
        } catch (\Throwable $error) {
            $this->disconnect();
            throw $error;
        }
    }

    /**
     * 在单消息窗口内获取下一条消息，并自动完成待发送 ACK/credit 与断线恢复。
     *
     * 只有等待新帧时的读超时返回 null；写入失败、EOF 等传输故障会恢复 stream，
     * 协议错误与状态机错误则直接向调用方暴露。
     */
    public function next(?int $timeoutMs = null): ?ConsumerRecord
    {
        $this->state->assertCanRead();

        $timeoutMs ??= 30000;
        $this->assertTimeout($timeoutMs);

        for (;;) {
            // 在单消息窗口下，只有 next() 可以授予新的投递额度。优先使用原子的
            // ACK+credit；Flow 只用于 open/rebalance 之后，或零额度控制屏障之后。
            $pendingAck = $this->state->pendingAck();
            if (null !== $pendingAck) {
                try {
                    $this->flushPendingAck(self::MESSAGE_CREDIT, $pendingAck->wireSize);
                } catch (ConsumerTransportException) {
                    // 超时写入可能只发送了部分帧，此时连接已不再满足帧边界安全，
                    // 因而绝不能像空闲读超时一样继续复用。
                    $this->reconnect();
                    continue;
                }
            } elseif (! $this->state->pendingCredit()->isEmpty()) {
                try {
                    $credit = $this->state->pendingCredit();
                    $this->send(
                        $this->protocol->flow($this->state->subscriptionId(), $this->state->epoch(), $credit->messages, $credit->bytes),
                        self::CONTROL_TIMEOUT_MS,
                    );
                    $this->state->confirmCreditFlushed();
                } catch (ConsumerTransportException) {
                    $this->reconnect();
                    continue;
                }
            }

            try {
                $decoded = $this->decode($this->receiveFrame($timeoutMs));
            } catch (ConsumerTransportException $error) {
                if ($error->isTimeout()) {
                    return null;
                }
                $this->reconnect();
                continue;
            } catch (KafkaException $error) {
                if (! $error->retryable) {
                    throw $error;
                }
                $this->reconnect();
                continue;
            }

            if ('message' === $decoded['kind']) {
                $message = $decoded['message'];
                if (! $message instanceof ConsumerRecord) {
                    throw new \RuntimeException('protocol returned an invalid ConsumerRecord');
                }
                $this->state->acceptMessage($message);
                return $message;
            }
            if ('assign' === $decoded['kind'] || 'revoke' === $decoded['kind']) {
                $this->acceptEpoch($decoded);
                continue;
            }
            if ('close' === $decoded['kind']) {
                $this->state->close();
                $this->disconnect();
                throw new \RuntimeException('consumer stream closed by worker');
            }
            $kind = (string) ($decoded['kind'] ?? 'unknown');
            throw new \RuntimeException("unexpected consumer stream event '{$kind}'");
        }
    }

    /**
     * 在本地确认当前消息并记录延迟 ACK；真正发送发生在下一个协议屏障。
     */
    public function ack(ConsumerRecord $message): void
    {
        $this->state->acknowledge($message);
    }

    /**
     * 校验并立即发送当前消息的 NACK，随后无论发送结果如何都终结本条 stream。
     */
    public function nack(ConsumerRecord $message, ?\Throwable $reason = null): void
    {
        // 接触网络前先校验消息。NACK 会终结当前 stream，因此即使传输失败，
        // finally 也必须释放本地所有权。
        $this->state->assertOutstanding($message);

        try {
            $this->send(
                $this->protocol->nack($this->state->subscriptionId(), $this->state->epoch(), $message->deliveryId(), $reason?->getMessage()),
                self::CONTROL_TIMEOUT_MS,
            );
        } finally {
            $this->state->settleNack($message);
            $this->disconnect();
        }
    }

    /**
     * 以零新增额度冲刷待发送 ACK，再提交 worker 维护的连续 ACK 水位。
     *
     * 方法等待并校验对应 Committed 响应；控制帧传输失败会丢弃连接，防止复用
     * correlation 状态不再可信的字节流。
     */
    public function commit(?int $timeoutMs = null): void
    {
        $this->state->assertOpen();
        $timeoutMs ??= self::CONTROL_TIMEOUT_MS;
        $this->assertTimeout($timeoutMs);

        try {
            // commit 读取水位前，ACK 必须先到达 actor。随 ACK 发送零额度，
            // 还能避免下一条消息抢在 commit 前投递。
            $this->flushPendingAck(0, 0);
            $request = $this->protocol->commit($this->state->subscriptionId(), $this->state->epoch());
            $this->send($request->frame, self::CONTROL_TIMEOUT_MS);
            do {
                $frame = $this->receiveFrame($timeoutMs);
                $decoded = $this->decode($frame);
                if (($decoded['kind'] ?? null) === 'assign' || ($decoded['kind'] ?? null) === 'revoke') {
                    $this->acceptEpoch($decoded);
                }
            } while (($decoded['kind'] ?? null) !== 'committed');
        } catch (ConsumerTransportException $error) {
            // 控制帧若只发送了一部分，就无法再可靠判断响应关联关系。丢弃连接，
            // 强制下一次操作从新的连接世代恢复。
            $this->disconnect();
            throw $error;
        }

        if ($frame->cid !== $request->cid) {
            throw new \RuntimeException('commit correlation id mismatch');
        }
        $this->state->assertIdentity(
            (int) ($decoded['subscription_id'] ?? 0),
            (int) ($decoded['epoch'] ?? 0),
            'commit response',
        );
    }

    /**
     * 通过零额度控制屏障暂停指定 partitions，不触发下一条消息投递。
     *
     * @param list<array{topic:string,partition:int}> $partitions 空数组表示当前完整 assignment
     */
    public function pause(array $partitions = []): void
    {
        $this->state->assertOpen();
        $this->sendBarrierControl(
            $this->protocol->pause($this->state->subscriptionId(), $this->state->epoch(), $partitions),
        );
    }

    /**
     * 通过零额度控制屏障恢复指定 partitions，不触发下一条消息投递。
     *
     * @param list<array{topic:string,partition:int}> $partitions 空数组表示当前完整 assignment
     */
    public function resume(array $partitions = []): void
    {
        $this->state->assertOpen();
        $this->sendBarrierControl(
            $this->protocol->resumePartitions($this->state->subscriptionId(), $this->state->epoch(), $partitions),
        );
    }

    /**
     * 先以零额度冲刷延迟 ACK，再发送无需响应的控制帧；传输失败时废弃连接。
     */
    private function sendBarrierControl(string $frame): void
    {
        try {
            $this->flushPendingAck(0, 0);
            $this->send($frame, self::CONTROL_TIMEOUT_MS);
        } catch (ConsumerTransportException $error) {
            $this->disconnect();
            throw $error;
        }
    }

    /**
     * 幂等关闭 subscription：冲刷 ACK、发送 ConsumeClose，并始终释放本地状态与连接。
     */
    public function close(): void
    {
        if ($this->state->isClosed()) {
            return;
        }

        try {
            $this->flushPendingAck(0, 0);
            $this->send($this->protocol->close($this->state->subscriptionId(), $this->state->epoch()), self::CLOSE_TIMEOUT_MS);
        } finally {
            $this->state->close();
            $this->disconnect();
        }
    }

    /**
     * 在对象回收时尽力关闭 stream，析构路径不向外传播异常。
     */
    public function __destruct()
    {
        try {
            $this->close();
        } catch (\Throwable) {
        }
    }

    /**
     * 返回当前有效的 subscription ID。
     */
    public function subscriptionId(): int
    {
        return $this->state->subscriptionId();
    }

    /**
     * 将完整协议帧写入当前 transport，统一保留毫秒超时语义。
     */
    private function send(string $frame, int $timeoutMs): void
    {
        $this->transport->sendAll($frame, $timeoutMs);
    }

    /**
     * 发送本地延迟 ACK，并按调用场景选择原子补充额度或保留待补充额度。
     */
    private function flushPendingAck(int $messageCredit, int $byteCredit): void
    {
        $ack = $this->state->pendingAck();
        if (null === $ack) {
            return;
        }
        $this->send(
            $this->protocol->ack(
                $this->state->subscriptionId(),
                $ack->epoch,
                $ack->deliveryId,
                $messageCredit,
                $byteCredit,
            ),
            self::CONTROL_TIMEOUT_MS,
        );
        // 零额度 flush 是控制屏障。将其待补充额度保留在本地，确保
        // commit/pause/close 不会意外触发下一条消息投递。
        $this->state->confirmAckFlushed(0 === $messageCredit);
    }

    /**
     * 从连接本地 frame reader 读取一个完整 wire frame。
     */
    private function receiveFrame(int $timeoutMs): ConsumerWireFrame
    {
        return $this->frames->read($timeoutMs);
    }

    /**
     * 解码 Consumer 帧；Error frame 在此统一转换为 KafkaException。
     *
     * @return array<string,mixed>
     */
    private function decode(ConsumerWireFrame $frame): array
    {
        if ($frame->kind === $this->protocol->errorFrameKind()) {
            throw $this->error($frame->raw);
        }
        return $this->protocol->decodeStreamFrame($frame->raw);
    }

    /**
     * 把原始 Error frame 解码并映射为稳定的 KafkaException。
     */
    private function error(string $frame): KafkaException
    {
        return KafkaExceptionFactory::fromDecoded($this->protocol->decodeErrorFrame($frame));
    }

    /**
     * 拒绝负数超时，零表示立即尝试且不等待。
     */
    private function assertTimeout(int $timeoutMs): void
    {
        if ($timeoutMs < 0) {
            throw new \InvalidArgumentException('timeout must not be negative');
        }
    }

    /**
     * 清空旧连接世代缓冲、建立 UDS 连接并完成 protocol-v2 HELLO 握手。
     */
    private function connectAndHello(): void
    {
        // 失败或只完成部分交互的连接可能遗留不完整帧；缓冲字节绝不能跨越连接世代。
        $this->frames->reset();
        $this->transport->connect($this->socket, $this->connectTimeoutMs);
        $this->send($this->protocol->helloFrame(), self::HELLO_TIMEOUT_MS);
        $hello = $this->receiveFrame(self::HELLO_TIMEOUT_MS);
        $this->protocol->verifyHello($hello->raw);
    }

    /**
     * 发送 ConsumeOpen 并安装 worker 返回的全新 subscription 身份与协商额度。
     */
    private function openFresh(): void
    {
        $open = $this->protocol->open(
            $this->cluster,
            $this->groupId,
            $this->topics,
            $this->config,
            self::MESSAGE_CREDIT,
            $this->byteCredit,
        );
        $this->send($open->frame, self::CONTROL_TIMEOUT_MS);
        $this->acceptReady($this->receiveFrame(self::CONTROL_TIMEOUT_MS), $open->cid);
    }

    /**
     * 在恢复宽限期内使用 resume token 重连既有 subscription。
     *
     * subscription 已不存在时创建新的 subscription；stale epoch 和临时传输失败在
     * 截止时间前退避重试，其他 Kafka 错误立即暴露。
     */
    private function reconnect(): void
    {
        $this->disconnect();
        $graceMs = (int) (\getenv('HI_KAFKA_CONSUMER_RESUME_GRACE_MS') ?: 10000);
        $deadline = \microtime(true) + ($graceMs / 1000);

        for (;;) {
            try {
                $this->worker->ensure($this->socket);
                $this->connectAndHello();
                $resume = $this->protocol->resume(
                    $this->state->subscriptionId(),
                    $this->state->epoch(),
                    $this->state->resumeToken(),
                );
                $this->send($resume->frame, self::CONTROL_TIMEOUT_MS);
                $this->acceptReady($this->receiveFrame(self::CONTROL_TIMEOUT_MS), $resume->cid);
                return;
            } catch (KafkaException $error) {
                if (self::ERROR_SUBSCRIPTION_NOT_FOUND === $error->kind) {
                    $this->disconnect();
                    $this->connectAndHello();
                    $this->openFresh();
                    return;
                }
                if (
                    (self::ERROR_STALE_EPOCH !== $error->kind && ! $error->retryable)
                    || \microtime(true) >= $deadline
                ) {
                    throw $error;
                }
            } catch (ConsumerTransportException $error) {
                if (\microtime(true) >= $deadline) {
                    throw $error;
                }
            }

            $this->disconnect();
            $this->transport->sleep(10);
        }
    }

    /**
     * 校验 ConsumeReady 的类型、cid 与顺序消费窗口，并安装协商后的有效 byte credit。
     */
    private function acceptReady(ConsumerWireFrame $frame, int $cid): void
    {
        $decoded = $this->decode($frame);
        if (($decoded['kind'] ?? null) !== 'ready' || $frame->cid !== $cid) {
            throw new \RuntimeException('invalid ConsumeReady response');
        }
        $maxMessages = (int) ($decoded['max_in_flight_messages'] ?? 0);
        $maxBytes = (int) ($decoded['max_in_flight_bytes'] ?? 0);
        if (self::MESSAGE_CREDIT !== $maxMessages || $maxBytes <= 0) {
            throw new \RuntimeException('worker negotiated an invalid sequential consumer window');
        }
        $this->negotiatedByteCredit = \min($this->byteCredit, $maxBytes);
        $this->state->installReady(
            (int) $decoded['subscription_id'],
            (int) $decoded['epoch'],
            (string) $decoded['resume_token'],
            new ConsumerCredit(self::MESSAGE_CREDIT, $this->negotiatedByteCredit),
        );
    }

    /**
     * 接受 assignment/revoke 通知，并在 epoch 变化时废止旧投递和延迟 ACK。
     *
     * @param array<string,mixed> $notice
     */
    private function acceptEpoch(array $notice): void
    {
        // epoch 变化会同时废止未确认投递与本地延迟 ACK；此后只有 worker 的水位可信。
        $this->state->acceptEpoch(
            (int) ($notice['subscription_id'] ?? 0),
            (int) ($notice['epoch'] ?? 0),
            new ConsumerCredit(self::MESSAGE_CREDIT, $this->negotiatedByteCredit),
        );
    }

    /**
     * 清除当前连接世代的 frame 缓冲并幂等关闭 transport。
     */
    private function disconnect(): void
    {
        $this->frames->reset();
        $this->transport->close();
    }
}
