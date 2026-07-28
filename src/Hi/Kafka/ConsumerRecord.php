<?php

declare(strict_types=1);

namespace Hi\Kafka;

// 扩展已注册原生类时不重复声明；扩展缺失时加载用户态兼容实现。
if (! \class_exists(ConsumerRecord::class, false)) {
    /**
     * Kafka Consumer 的不可变单条投递记录。
     *
     * 原生实例由 ext-kafka 创建，并携带 ConsumerStream::ack()/nack() 所需的私有
     * 投递身份。扩展不可用时，此用户态实现也是测试及伪驱动使用的标准记录对象；
     * 原生 ConsumerStream 仍会校验私有身份，因此无法接受伪造记录。
     */
    class ConsumerRecord
    {
        /**
         * 构造测试或伪驱动使用的不可变消息记录。
         *
         * @param list<array{name:string,value:string}> $headers 保持原始顺序且允许重名的 Kafka 消息头
         */
        public function __construct(
            private readonly int $subscriptionId,
            private readonly int $subscriptionEpoch,
            private readonly int $deliveryId,
            private readonly string $topic,
            private readonly int $partition,
            private readonly int $offset,
            private readonly int $timestampMs,
            private readonly int $wireSize,
            private readonly ?string $key,
            private readonly ?string $value,
            private readonly array $headers = [],
        ) {
        }

        /**
         * 返回拥有本次投递的 subscription ID。
         */
        public function subscriptionId(): int
        {
            return $this->subscriptionId;
        }

        /**
         * 返回本次投递所属的 subscription epoch。
         */
        public function subscriptionEpoch(): int
        {
            return $this->subscriptionEpoch;
        }

        /**
         * 返回当前 epoch 内唯一的投递 ID，供 ACK/NACK 校验。
         */
        public function deliveryId(): int
        {
            return $this->deliveryId;
        }

        /**
         * 返回消息所属主题。
         */
        public function topic(): string
        {
            return $this->topic;
        }

        /**
         * 返回消息所属分区。
         */
        public function partition(): int
        {
            return $this->partition;
        }

        /**
         * 返回消息在分区中的 broker offset。
         */
        public function offset(): int
        {
            return $this->offset;
        }

        /**
         * 返回 Kafka 消息时间戳，单位为毫秒。
         */
        public function timestampMs(): int
        {
            return $this->timestampMs;
        }

        /**
         * 返回该消息占用的协议信用字节数，用于精确补充 byte credit。
         */
        public function wireSize(): int
        {
            return $this->wireSize;
        }

        /**
         * 返回消息 key；Kafka null key 保持为 null。
         */
        public function key(): ?string
        {
            return $this->key;
        }

        /**
         * 返回消息 value；Kafka null value 保持为 null。
         */
        public function value(): ?string
        {
            return $this->value;
        }

        /**
         * 返回保持原始顺序与重复名称的 Kafka 消息头。
         *
         * @return list<array{name:string,value:string}>
         */
        public function headers(): array
        {
            return $this->headers;
        }
    }
}
