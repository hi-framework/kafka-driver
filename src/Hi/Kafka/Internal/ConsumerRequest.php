<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * 带 correlation ID 的已编码 Consumer 请求。
 *
 * @internal
 */
final readonly class ConsumerRequest
{
    /**
     * 将请求 correlation ID 与其完整 wire frame 绑定，供响应校验使用。
     */
    public function __construct(
        public int $cid,
        public string $frame,
    ) {
    }
}
