<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * @internal 不持有消息 payload 的 ACK 意图，保留到下一个协议屏障再发送
 */
final readonly class PendingConsumerAck
{
    /**
     * 保存发送 ACK 所需的最小身份与补充额度信息，不保留消息 payload。
     */
    public function __construct(
        public int $epoch,
        public int $deliveryId,
        public int $wireSize,
    ) {
        if ($epoch <= 0 || $deliveryId <= 0 || $wireSize < 0) {
            throw new \InvalidArgumentException('invalid pending consumer ACK');
        }
    }
}
