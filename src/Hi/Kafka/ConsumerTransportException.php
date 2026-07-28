<?php

declare(strict_types=1);

namespace Hi\Kafka;

/**
 * 归一化后的协程传输异常。
 *
 * 仅此异常允许 CoroutineConsumerStream 触发读超时返回或断线恢复；协议错误、
 * correlation 错误和状态机错误必须以各自原始异常暴露。
 *
 * @internal
 */
final class ConsumerTransportException extends \RuntimeException
{
    /**
     * 创建传输异常，并显式记录该失败是否属于超时。
     */
    public function __construct(
        string $message,
        int $code = 0,
        ?\Throwable $previous = null,
        private readonly bool $timeout = false,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * 判断该传输失败是否由连接、读取或写入超时引起。
     */
    public function isTimeout(): bool
    {
        return $this->timeout;
    }
}
