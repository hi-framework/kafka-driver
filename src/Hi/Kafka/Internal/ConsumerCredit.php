<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * 一次待授予 worker 的消息数与字节数信用额度。
 *
 * @internal
 */
final readonly class ConsumerCredit
{
    /**
     * 创建非负信用额度；任一维度为负数均表示调用方破坏背压不变量。
     */
    public function __construct(
        public int $messages,
        public int $bytes,
    ) {
        if ($messages < 0 || $bytes < 0) {
            throw new \InvalidArgumentException('consumer credit must not be negative');
        }
    }

    /**
     * 判断消息和字节两个维度是否都无需补充。
     */
    public function isEmpty(): bool
    {
        return 0 === $this->messages && 0 === $this->bytes;
    }
}
