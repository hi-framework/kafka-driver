<?php

declare(strict_types=1);

namespace Hi\Kafka\Tests;

use Hi\Kafka\ConsumerRecord;
use Hi\Kafka\Internal\ConsumerCredit;
use Hi\Kafka\Internal\ConsumerStreamState;
use PHPUnit\Framework\TestCase;

final class ConsumerStreamStateTest extends TestCase
{
    private ConsumerStreamState $state;

    protected function setUp(): void
    {
        $this->state = new ConsumerStreamState;
        $this->state->installReady(7, 1, 'resume-token', new ConsumerCredit(1, 1024));
    }

    public function testAckIntentIsPayloadFreeAndCanAtomicallyReplenish(): void
    {
        $message = $this->record(deliveryId: 11, wireSize: 128, value: \str_repeat('x', 4096));
        $this->state->confirmCreditFlushed();
        $this->state->acceptMessage($message);
        $this->state->acknowledge($message);

        $ack = $this->state->pendingAck();
        self::assertNotNull($ack);
        self::assertSame(1, $ack->epoch);
        self::assertSame(11, $ack->deliveryId);
        self::assertSame(128, $ack->wireSize);

        $this->state->confirmAckFlushed(false);
        self::assertNull($this->state->pendingAck());
        self::assertTrue($this->state->pendingCredit()->isEmpty());
    }

    public function testZeroCreditBarrierPreservesReplenishmentForLaterNext(): void
    {
        $message = $this->record(deliveryId: 12, wireSize: 256);
        $this->state->confirmCreditFlushed();
        $this->state->acceptMessage($message);
        $this->state->acknowledge($message);

        $this->state->confirmAckFlushed(true);

        self::assertSame(1, $this->state->pendingCredit()->messages);
        self::assertSame(256, $this->state->pendingCredit()->bytes);
    }

    public function testStaleMessageIsRejectedBeforeItReachesApplicationCode(): void
    {
        $this->state->confirmCreditFlushed();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('another stream epoch');
        $this->state->acceptMessage($this->record(epoch: 2));
    }

    public function testEpochChangeInvalidatesOutstandingDeliveryAndPendingAck(): void
    {
        $message = $this->record();
        $this->state->confirmCreditFlushed();
        $this->state->acceptMessage($message);
        $this->state->acknowledge($message);

        $this->state->acceptEpoch(7, 2, new ConsumerCredit(1, 2048));

        self::assertNull($this->state->pendingAck());
        self::assertSame(2, $this->state->epoch());
        self::assertSame(2048, $this->state->pendingCredit()->bytes);
    }

    private function record(
        int $subscriptionId = 7,
        int $epoch = 1,
        int $deliveryId = 10,
        int $wireSize = 64,
        string $value = 'value',
    ): ConsumerRecord {
        return new ConsumerRecord(
            $subscriptionId,
            $epoch,
            $deliveryId,
            'topic',
            0,
            1,
            0,
            $wireSize,
            'key',
            $value,
        );
    }
}
