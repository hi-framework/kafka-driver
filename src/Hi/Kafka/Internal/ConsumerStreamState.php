<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

use Hi\Kafka\ConsumerRecord;

/**
 * @internal 纯订阅状态机。负责身份、信用额度与单条投递确认不变量，
 * 不执行 IO，也不参与协议编码。
 */
final class ConsumerStreamState
{
    private ?int $subscriptionId = null;
    private ?int $epoch = null;
    private ?string $resumeToken = null;
    private ConsumerCredit $pendingCredit;
    private ?int $outstandingDeliveryId = null;
    private ?PendingConsumerAck $pendingAck = null;
    private bool $closed = true;

    /**
     * 创建尚未安装 ConsumeReady 身份的关闭状态机，初始待补充额度为零。
     */
    public function __construct()
    {
        $this->pendingCredit = new ConsumerCredit(0, 0);
    }

    /**
     * 安装 ConsumeReady 返回的新身份，并清除旧投递、ACK 与额度状态。
     */
    public function installReady(int $subscriptionId, int $epoch, string $resumeToken, ConsumerCredit $initialCredit): void
    {
        if ($subscriptionId <= 0 || $epoch <= 0 || '' === $resumeToken) {
            throw new \RuntimeException('invalid ConsumeReady identity');
        }
        $this->subscriptionId = $subscriptionId;
        $this->epoch = $epoch;
        $this->resumeToken = $resumeToken;
        $this->pendingCredit = $initialCredit;
        $this->outstandingDeliveryId = null;
        $this->pendingAck = null;
        $this->closed = false;
    }

    /**
     * 断言 stream 已打开且不存在必须先 ACK/NACK 的未确认消息。
     */
    public function assertCanRead(): void
    {
        $this->assertOpen();
        if (null !== $this->outstandingDeliveryId) {
            throw new \LogicException('ack or nack the current message before calling next');
        }
    }

    /**
     * 接纳 worker 交付的消息，在校验 stream 身份后将其登记为唯一未确认投递。
     */
    public function acceptMessage(ConsumerRecord $message): void
    {
        $this->assertCanRead();
        $this->assertCurrentIdentity($message);
        if (null !== $this->pendingAck) {
            throw new \RuntimeException('consumer stream received a message before pending ACK was flushed');
        }
        $this->outstandingDeliveryId = $message->deliveryId();
    }

    /**
     * 将当前消息转换为不持有 payload 的延迟 ACK 意图，并释放本地未确认槽位。
     */
    public function acknowledge(ConsumerRecord $message): void
    {
        $this->assertCurrentDelivery($message);
        $this->pendingAck = new PendingConsumerAck(
            $message->subscriptionEpoch(),
            $message->deliveryId(),
            $message->wireSize(),
        );
        $this->outstandingDeliveryId = null;
    }

    /**
     * 确认当前消息已执行 NACK，并将本地 stream 标记为终结状态。
     */
    public function settleNack(ConsumerRecord $message): void
    {
        $this->assertCurrentDelivery($message);
        $this->outstandingDeliveryId = null;
        $this->closed = true;
    }

    /**
     * 校验消息正是当前 stream 唯一尚未确认的投递，不改变状态。
     */
    public function assertOutstanding(ConsumerRecord $message): void
    {
        $this->assertCurrentDelivery($message);
    }

    /**
     * 返回尚未到达下一协议屏障的延迟 ACK 意图。
     */
    public function pendingAck(): ?PendingConsumerAck
    {
        return $this->pendingAck;
    }

    /**
     * 确认 ACK 已写入连接并清除意图；零额度屏障下把对应额度保留为本地待补充量。
     */
    public function confirmAckFlushed(bool $preserveCredit): void
    {
        $ack = $this->pendingAck;
        if (null === $ack) {
            return;
        }
        $this->pendingAck = null;
        if ($preserveCredit) {
            $this->pendingCredit = new ConsumerCredit(
                $this->pendingCredit->messages + 1,
                $this->pendingCredit->bytes + $ack->wireSize,
            );
        }
    }

    /**
     * 返回下一次 FlowControl 需要补充的本地累计额度。
     */
    public function pendingCredit(): ConsumerCredit
    {
        return $this->pendingCredit;
    }

    /**
     * 确认待补充额度已写入连接，并把本地累计量归零。
     */
    public function confirmCreditFlushed(): void
    {
        $this->pendingCredit = new ConsumerCredit(0, 0);
    }

    /**
     * 接受 rebalance epoch 通知；epoch 变化时废止旧投递与 ACK，并安装新额度。
     */
    public function acceptEpoch(int $subscriptionId, int $epoch, ConsumerCredit $initialCredit): void
    {
        $this->assertOpen();
        if ($subscriptionId !== $this->subscriptionId) {
            throw new \RuntimeException('rebalance notice belongs to another stream');
        }
        if ($epoch <= 0) {
            throw new \RuntimeException('invalid rebalance epoch');
        }
        if ($epoch !== $this->epoch) {
            $this->epoch = $epoch;
            $this->outstandingDeliveryId = null;
            $this->pendingAck = null;
            $this->pendingCredit = $initialCredit;
        }
    }

    /**
     * 在本地幂等关闭 stream，并废止尚未确认的投递与延迟 ACK。
     */
    public function close(): void
    {
        $this->closed = true;
        $this->outstandingDeliveryId = null;
        $this->pendingAck = null;
    }

    /**
     * 断言状态机已安装有效身份且尚未关闭。
     */
    public function assertOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('consumer stream is closed');
        }
    }

    /**
     * 校验响应或通知属于当前 subscription ID 与 epoch。
     */
    public function assertIdentity(int $subscriptionId, int $epoch, string $source): void
    {
        $this->assertOpen();
        if ($subscriptionId !== $this->subscriptionId || $epoch !== $this->epoch) {
            throw new \RuntimeException("{$source} belongs to another stream epoch");
        }
    }

    /**
     * 返回当前 subscription ID；尚未 ready 时拒绝访问。
     */
    public function subscriptionId(): int
    {
        return $this->subscriptionId ?? throw new \LogicException('consumer stream is not ready');
    }

    /**
     * 返回当前 subscription epoch；尚未 ready 时拒绝访问。
     */
    public function epoch(): int
    {
        return $this->epoch ?? throw new \LogicException('consumer stream is not ready');
    }

    /**
     * 返回仅用于断线恢复的 subscription resume token。
     */
    public function resumeToken(): string
    {
        return $this->resumeToken ?? throw new \LogicException('consumer stream is not ready');
    }

    /**
     * 判断本地 stream 是否处于关闭状态。
     */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * 同时校验消息身份和 delivery ID 是否匹配当前未确认投递。
     */
    private function assertCurrentDelivery(ConsumerRecord $message): void
    {
        $this->assertCurrentIdentity($message);
        if ($this->outstandingDeliveryId !== $message->deliveryId()) {
            throw new \LogicException('message is not the current outstanding delivery');
        }
    }

    /**
     * 校验消息由当前 subscription ID 与 epoch 交付，拒绝跨 stream 或过期消息。
     */
    private function assertCurrentIdentity(ConsumerRecord $message): void
    {
        $this->assertOpen();
        if ($message->subscriptionId() !== $this->subscriptionId || $message->subscriptionEpoch() !== $this->epoch) {
            throw new \InvalidArgumentException('message belongs to another stream epoch');
        }
    }
}
