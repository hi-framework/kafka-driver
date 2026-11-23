<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

use Hi\Kafka\ConsumerTransportException;
use Hi\Kafka\ConsumerTransportInterface;
use Hi\Kafka\KafkaException;

/**
 * Swoole 与 Swow 共用的协程 RPC 连接池与请求执行器。
 *
 * runtime 差异只存在于 {@see ConsumerTransportInterface} adapter；连接池、fork 检测、
 * 拆帧、错误分类与 worker 故障恢复只有这一份实现。
 *
 * 恢复策略（每类恢复每次调用最多一次，避免故障时形成重试风暴）：
 * - 传输失败（连接被拒、EOF、EPIPE）：说明 worker 已死或已重启。丢弃全部空闲连接，
 *   强制探测并在需要时重新拉起 worker（扩展会重放本进程记录的集群），再重试一次。
 * - `CLUSTER_NOT_REGISTERED`：worker 被其它进程重启过，本进程的集群不在新 worker
 *   中。重放后重试一次；该错误在执行前判定，重试不会产生重复消息。
 * - 超时不重试：请求可能已经执行，结果未知，交给调用方决定。
 *
 * @internal
 */
final class CoroutineRpcClient
{
    private const ERROR_CLUSTER_NOT_REGISTERED = 5;
    private const HANDSHAKE_TIMEOUT_MS = 2000;

    /**
     * @var list<array{0:ConsumerTransportInterface,1:ConsumerFrameReader}>
     */
    private array $idle = [];
    private int $ownerPid;
    private int $created = 0;
    private bool $workerEnsured = false;

    /**
     * @param \Closure(): ConsumerTransportInterface $transportFactory 每次建链创建一个新 adapter
     */
    public function __construct(
        private readonly string $socket,
        private readonly \Closure $transportFactory,
        private readonly int $maxIdle,
        private readonly int $connectTimeoutMs,
        private readonly ConsumerProtocolInterface $protocol,
        private readonly ConsumerWorkerInterface $worker,
    ) {
        if ($maxIdle < 0) {
            throw new \InvalidArgumentException('maxIdle must not be negative');
        }
        $this->ownerPid = (int) \getmypid();
    }

    /**
     * 执行一次请求并返回完整的响应帧（header + payload）。
     *
     * worker 返回的 Error 帧转换为 {@see KafkaException}，连接保持可复用；
     * 传输与帧错误会丢弃该连接。
     *
     * @param \Closure(): ConsumerRequest $encode 每次尝试都重新编码，保证重试使用新 cid
     */
    public function call(\Closure $encode, int $timeoutMs): string
    {
        // 两类恢复各自最多一次：worker 被别的进程重启时，先因旧连接 EOF 触发传输
        // 恢复，重试后又会遇到 CLUSTER_NOT_REGISTERED，需要再做一次重放。
        $reconnected = false;
        $replayed = false;
        for (;;) {
            $request = $encode();

            try {
                return $this->roundTrip($request, $timeoutMs);
            } catch (ConsumerTransportException $error) {
                if ($reconnected || $error->isTimeout()) {
                    throw $error;
                }
                $reconnected = true;
                $this->discardIdle();
                $this->worker->ensure($this->socket);
                $this->workerEnsured = true;
            } catch (KafkaException $error) {
                if ($replayed || self::ERROR_CLUSTER_NOT_REGISTERED !== $error->kind) {
                    throw $error;
                }
                $replayed = true;
                $this->worker->replay($this->socket);
            }
        }
    }

    /**
     * 显式确保 worker 存活（强制探测，不走缓存）。
     */
    public function ensureWorker(): void
    {
        $this->worker->ensure($this->socket);
        $this->workerEnsured = true;
    }

    /**
     * 关闭全部空闲连接；在用连接由持有者归还或丢弃。
     */
    public function close(): void
    {
        $this->discardIdle();
    }

    /**
     * @return array{socket:string,max_idle:int,idle:int,created:int}
     */
    public function stats(): array
    {
        $this->checkOwner();

        return [
            'socket' => $this->socket,
            'max_idle' => $this->maxIdle,
            'idle' => \count($this->idle),
            'created' => $this->created,
        ];
    }

    public function worker(): ConsumerWorkerInterface
    {
        return $this->worker;
    }

    private function roundTrip(ConsumerRequest $request, int $timeoutMs): string
    {
        [$transport, $frames] = $this->acquire();

        try {
            $transport->sendAll($request->frame, $timeoutMs);
            $frame = $frames->read($timeoutMs);
        } catch (\Throwable $error) {
            $transport->close();
            throw $error;
        }

        if ($frame->cid !== $request->cid) {
            $transport->close();
            throw new \RuntimeException("cid mismatch: sent {$request->cid}, got {$frame->cid}");
        }

        $this->release($transport, $frames);
        if ($frame->kind === $this->protocol->errorFrameKind()) {
            throw KafkaExceptionFactory::fromDecoded($this->protocol->decodeErrorFrame($frame->raw));
        }

        return $frame->raw;
    }

    /**
     * @return array{0:ConsumerTransportInterface,1:ConsumerFrameReader}
     */
    private function acquire(): array
    {
        $this->checkOwner();

        return \array_pop($this->idle) ?? $this->connect();
    }

    private function release(ConsumerTransportInterface $transport, ConsumerFrameReader $frames): void
    {
        if (\count($this->idle) >= $this->maxIdle) {
            $transport->close();
            return;
        }
        $this->idle[] = [$transport, $frames];
    }

    /**
     * @return array{0:ConsumerTransportInterface,1:ConsumerFrameReader}
     */
    private function connect(): array
    {
        if (! $this->workerEnsured) {
            $this->worker->ensure($this->socket);
            $this->workerEnsured = true;
        }

        $transport = ($this->transportFactory)();
        $frames = new ConsumerFrameReader($transport, $this->protocol);

        try {
            $transport->connect($this->socket, $this->connectTimeoutMs);
            $transport->sendAll($this->protocol->rpcHelloFrame(), self::HANDSHAKE_TIMEOUT_MS);
            $this->protocol->verifyHello($frames->read(self::HANDSHAKE_TIMEOUT_MS)->raw);
        } catch (\Throwable $error) {
            $transport->close();
            throw $error;
        }

        ++$this->created;
        return [$transport, $frames];
    }

    /**
     * fork 后子进程绝不能复用父进程的 UDS fd：两个进程读写同一条字节流会让
     * 响应被另一方读走。检测到 PID 变化就放弃继承的全部连接与状态。
     */
    private function checkOwner(): void
    {
        $pid = (int) \getmypid();
        if ($pid === $this->ownerPid) {
            return;
        }
        $this->ownerPid = $pid;
        $this->discardIdle();
        $this->created = 0;
        $this->workerEnsured = false;
    }

    private function discardIdle(): void
    {
        $idle = $this->idle;
        $this->idle = [];
        foreach ($idle as [$transport]) {
            $transport->close();
        }
    }
}
