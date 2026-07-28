<?php

declare(strict_types=1);

namespace Hi\Kafka;

if (! \interface_exists(ConsumerStreamInterface::class, false)) {
    /**
     * protocol-v2 的单消息消费流契约。
     *
     * stream 同一时刻最多向业务交付一条未确认消息。调用方必须对当前消息执行
     * ack() 或 nack() 后才能继续 next()，不得把它当作批量 poll API 使用。
     */
    interface ConsumerStreamInterface
    {
        /**
         * 等待并返回下一条可交付消息；等待窗口内无消息时返回 null。
         *
         * null 仅表示本次读取超时，stream 与 subscription 仍保持有效。
         */
        public function next(?int $timeoutMs = null): ?ConsumerRecord;

        /**
         * 标记当前消息已被业务成功处理，并延迟到下一个协议屏障发送 ACK。
         */
        public function ack(ConsumerRecord $message): void;

        /**
         * 拒绝当前消息并终止本条 stream，附带的异常仅作为失败原因传给 worker。
         */
        public function nack(ConsumerRecord $message, ?\Throwable $reason = null): void;

        /**
         * 先冲刷本地待发送 ACK，再让 broker 提交连续 ACK 水位对应的 offsets。
         */
        public function commit(?int $timeoutMs = null): void;

        /**
         * 暂停当前 epoch 中指定的 assignment partitions；空数组表示完整 assignment。
         *
         * @param list<array{topic:string,partition:int}> $partitions
         */
        public function pause(array $partitions = []): void;

        /**
         * 恢复当前 epoch 中指定的 assignment partitions；空数组表示完整 assignment。
         *
         * @param list<array{topic:string,partition:int}> $partitions
         */
        public function resume(array $partitions = []): void;

        /**
         * 冲刷待发送 ACK、通知 worker 关闭 subscription，并释放独占连接。
         */
        public function close(): void;

        /**
         * 返回 worker 为当前 stream 分配的 subscription ID。
         */
        public function subscriptionId(): int;
    }
}
