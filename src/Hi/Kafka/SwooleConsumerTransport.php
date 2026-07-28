<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Swoole\Coroutine\Socket;

/**
 * 基于 Swoole Coroutine Socket 的 Consumer 独占连接适配器。
 *
 * 只负责毫秒超时换算、完整写入、增量读取和异常归一化，不持有任何消费协议状态。
 *
 * @internal
 */
final class SwooleConsumerTransport implements ConsumerTransportInterface
{
    private const AF_UNIX = 1;
    private const SOCK_STREAM = 1;

    private ?Socket $connection = null;

    /**
     * 丢弃旧连接并建立新的 Unix Domain Socket 连接。
     */
    public function connect(string $socket, int $timeoutMs): void
    {
        $this->close();
        $connection = new Socket(self::AF_UNIX, self::SOCK_STREAM, 0);
        $this->connection = $connection;

        try {
            if (! $connection->connect($socket, 0, $timeoutMs / 1000)) {
                throw $this->failure("connect {$socket} failed", $connection->errCode, $connection->errMsg);
            }
        } catch (ConsumerTransportException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw $this->fromThrowable("connect {$socket} failed", $error);
        }
    }

    /**
     * 在给定超时内完整写出一个协议帧；短写视为传输失败。
     */
    public function sendAll(string $data, int $timeoutMs): void
    {
        $connection = $this->connection();

        try {
            $written = $connection->sendAll($data, $timeoutMs / 1000);
            if (false === $written || $written !== \strlen($data)) {
                throw $this->failure('consumer transport write failed', $connection->errCode, $connection->errMsg);
            }
        } catch (ConsumerTransportException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw $this->fromThrowable('consumer transport write failed', $error);
        }
    }

    /**
     * 从当前连接增量读取最多指定字节；空串按对端 EOF 处理。
     */
    public function receive(int $maxBytes, int $timeoutMs): string
    {
        $connection = $this->connection();

        try {
            $data = $connection->recv($maxBytes, $timeoutMs / 1000);
            if (false === $data) {
                throw $this->failure('consumer transport read failed', $connection->errCode, $connection->errMsg);
            }
            if ('' === $data) {
                throw new ConsumerTransportException('consumer transport reached EOF');
            }
            return $data;
        } catch (ConsumerTransportException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw $this->fromThrowable('consumer transport read failed', $error);
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
     * 通过 Swoole 调度器进行非阻塞毫秒级休眠。
     */
    public function sleep(int $milliseconds): void
    {
        \Swoole\Coroutine::sleep($milliseconds / 1000);
    }

    /**
     * 返回当前活动 socket；未连接时抛出归一化传输异常。
     */
    private function connection(): Socket
    {
        return $this->connection ?? throw new ConsumerTransportException('consumer transport is not connected');
    }

    /**
     * 将 Swoole 错误码与错误文本转换为带超时分类的传输异常。
     */
    private function failure(string $operation, int $code, string $detail): ConsumerTransportException
    {
        $message = '' === $detail ? $operation : "{$operation}: {$detail}";
        return new ConsumerTransportException($message, $code, null, $this->isTimeout($code, $message));
    }

    /**
     * 保留原异常链，并将任意运行时异常归一化为传输异常。
     */
    private function fromThrowable(string $operation, \Throwable $error): ConsumerTransportException
    {
        $message = "{$operation}: {$error->getMessage()}";
        return new ConsumerTransportException($message, (int) $error->getCode(), $error, $this->isTimeout((int) $error->getCode(), $message));
    }

    /**
     * 兼容常见平台 errno 与 Swoole 错误文本，判断失败是否属于超时。
     */
    private function isTimeout(int $code, string $message): bool
    {
        return \in_array(\abs($code), [11, 35, 60, 110], true)
            || \str_contains(\strtolower($message), 'timed out')
            || \str_contains(\strtolower($message), 'timeout');
    }
}
