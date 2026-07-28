<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Swow\Socket;

/**
 * 基于 Swow Socket 的 Consumer 独占连接适配器。
 *
 * 只负责毫秒到微秒的换算、完整写入、增量读取和异常归一化，不持有消费协议状态。
 *
 * @internal
 */
final class SwowConsumerTransport implements ConsumerTransportInterface
{
    private ?Socket $connection = null;

    /**
     * 丢弃旧连接并建立新的 Unix Domain Socket 连接。
     */
    public function connect(string $socket, int $timeoutMs): void
    {
        $this->close();

        try {
            $connection = new Socket(Socket::TYPE_UNIX);
            $this->connection = $connection;
            $connection->connect($socket, 0, $this->microseconds($timeoutMs));
        } catch (\Throwable $error) {
            throw $this->failure("connect {$socket} failed", $error);
        }
    }

    /**
     * 在给定超时内依赖 Swow 的完整发送语义写出一个协议帧。
     */
    public function sendAll(string $data, int $timeoutMs): void
    {
        try {
            $this->connection()->send($data, 0, -1, $this->microseconds($timeoutMs));
        } catch (\Throwable $error) {
            throw $this->failure('consumer transport write failed', $error);
        }
    }

    /**
     * 从当前连接增量读取最多指定字节；空串按对端 EOF 处理。
     */
    public function receive(int $maxBytes, int $timeoutMs): string
    {
        try {
            $data = $this->connection()->recvString($maxBytes, $this->microseconds($timeoutMs));
            if ('' === $data) {
                throw new ConsumerTransportException('consumer transport reached EOF');
            }
            return $data;
        } catch (ConsumerTransportException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw $this->failure('consumer transport read failed', $error);
        }
    }

    /**
     * 幂等关闭当前连接，并吞掉析构或恢复路径上的关闭错误。
     */
    public function close(): void
    {
        $connection = $this->connection;
        $this->connection = null;
        if (null === $connection) {
            return;
        }

        try {
            $connection->close();
        } catch (\Throwable) {
        }
    }

    /**
     * 通过 Swow 调度器进行非阻塞毫秒级休眠。
     */
    public function sleep(int $milliseconds): void
    {
        \Swow\Coroutine::msleep($milliseconds);
    }

    /**
     * 返回当前活动 socket；未连接时抛出归一化传输异常。
     */
    private function connection(): Socket
    {
        return $this->connection ?? throw new ConsumerTransportException('consumer transport is not connected');
    }

    /**
     * 将统一的毫秒超时转换为 Swow API 使用的微秒。
     */
    private function microseconds(int $milliseconds): int
    {
        return $milliseconds * 1000;
    }

    /**
     * 保留原异常链，并按 errno 与错误文本标记是否超时。
     */
    private function failure(string $operation, \Throwable $error): ConsumerTransportException
    {
        $code = (int) $error->getCode();
        $message = "{$operation}: {$error->getMessage()}";
        $timeout = \in_array(\abs($code), [11, 35, 60, 110], true)
            || \str_contains(\strtolower($message), 'timed out')
            || \str_contains(\strtolower($message), 'timeout');
        return new ConsumerTransportException($message, $code, $error, $timeout);
    }
}
