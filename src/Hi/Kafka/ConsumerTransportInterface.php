<?php

declare(strict_types=1);

namespace Hi\Kafka;

/**
 * CoroutineConsumerStream 使用的协程运行时传输适配边界。
 *
 * Swoole 与 Swow 只在此层处理 API 和时间单位差异；所有超时参数统一使用毫秒，
 * 所有失败统一转换为 ConsumerTransportException，状态机和协议语义不得下沉到适配器。
 *
 * @internal
 */
interface ConsumerTransportInterface
{
    /**
     * 关闭已有连接后，建立一条新的 worker UDS 连接。
     */
    public function connect(string $socket, int $timeoutMs): void;

    /**
     * 完整写出给定字节串；短写、超时或其他 IO 错误均抛出传输异常。
     */
    public function sendAll(string $data, int $timeoutMs): void;

    /**
     * 读取 1 至 $maxBytes 个字节；EOF、超时或其他 IO 错误均抛出传输异常。
     */
    public function receive(int $maxBytes, int $timeoutMs): string;

    /**
     * 尽力关闭当前连接；允许重复调用且不向上抛出关闭错误。
     */
    public function close(): void;

    /**
     * 以协程友好的方式暂停指定毫秒数，用于断线恢复退避。
     */
    public function sleep(int $milliseconds): void;
}
