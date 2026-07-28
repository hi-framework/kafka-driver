<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * 已按 envelope 边界完整读取、但尚未解码 payload 的 Consumer 协议帧。
 *
 * @internal
 */
final readonly class ConsumerWireFrame
{
    /**
     * 保存帧类型、correlation ID 和包含 header 的完整原始字节。
     */
    public function __construct(
        public int $kind,
        public int $cid,
        public string $raw,
    ) {
    }
}
