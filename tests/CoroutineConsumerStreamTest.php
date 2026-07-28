<?php

declare(strict_types=1);

namespace Hi\Kafka\Tests;

use Hi\Kafka\ConsumerRecord;
use Hi\Kafka\ConsumerTransportException;
use Hi\Kafka\ConsumerTransportInterface;
use Hi\Kafka\CoroutineConsumerStream;
use Hi\Kafka\Internal\ConsumerProtocolInterface;
use Hi\Kafka\Internal\ConsumerRequest;
use Hi\Kafka\Internal\ConsumerWorkerInterface;
use PHPUnit\Framework\TestCase;

final class CoroutineConsumerStreamTest extends TestCase
{
    public function testNextCombinesDeferredAckWithExactCredit(): void
    {
        $protocol = new FakeConsumerProtocol;
        $transport = new FakeConsumerTransport(
            $protocol->wire(['kind' => 'hello'])
            . $protocol->wire($this->ready(), cid: 10)
            . $protocol->wire($this->message(11, 128)),
        );
        $worker = new FakeConsumerWorker;
        $stream = $this->stream($transport, $protocol, $worker);

        $first = $stream->next(100);
        self::assertSame(11, $first?->deliveryId());
        $stream->ack($first);
        self::assertSame(['HELLO', 'OPEN', 'FLOW:7:1:1:1024'], $transport->sent);

        $transport->append($protocol->wire($this->message(12, 256)));
        $second = $stream->next(100);

        self::assertSame(12, $second?->deliveryId());
        self::assertSame('ACK:7:1:11:1:128', $transport->sent[3]);
        self::assertSame(1, $worker->ensureCalls);
        $stream->nack($second);
    }

    public function testCommitFlushesZeroCreditAndNextRestoresItExplicitly(): void
    {
        $protocol = new FakeConsumerProtocol;
        $transport = new FakeConsumerTransport(
            $protocol->wire(['kind' => 'hello'])
            . $protocol->wire($this->ready(), cid: 10)
            . $protocol->wire($this->message(21, 300)),
        );
        $stream = $this->stream($transport, $protocol, new FakeConsumerWorker);
        $message = $stream->next(100);
        $stream->ack($message);

        $transport->append($protocol->wire([
            'kind' => 'committed',
            'subscription_id' => 7,
            'epoch' => 1,
        ], cid: 12));
        $stream->commit(100);

        self::assertSame('ACK:7:1:21:0:0', $transport->sent[3]);
        self::assertSame('COMMIT:7:1', $transport->sent[4]);

        $transport->append($protocol->wire($this->message(22, 301)));
        self::assertSame(22, $stream->next(100)?->deliveryId());
        self::assertSame('FLOW:7:1:1:300', $transport->sent[5]);
    }

    public function testIdleReadTimeoutReturnsNullWithoutReconnecting(): void
    {
        $protocol = new FakeConsumerProtocol;
        $transport = new FakeConsumerTransport(
            $protocol->wire(['kind' => 'hello']) . $protocol->wire($this->ready(), cid: 10),
        );
        $stream = $this->stream($transport, $protocol, new FakeConsumerWorker);

        self::assertNull($stream->next(1));
        self::assertSame(1, $transport->connectCalls);
    }

    public function testReadyNegotiationCapsTheConfiguredByteWindow(): void
    {
        $protocol = new FakeConsumerProtocol;
        $transport = new FakeConsumerTransport(
            $protocol->wire(['kind' => 'hello'])
            . $protocol->wire($this->ready(maxBytes: 512), cid: 10)
            . $protocol->wire($this->message(31, 128)),
        );
        $stream = $this->stream($transport, $protocol, new FakeConsumerWorker);

        self::assertSame(31, $stream->next(100)?->deliveryId());
        self::assertSame('FLOW:7:1:1:512', $transport->sent[2]);
    }

    private function stream(
        FakeConsumerTransport $transport,
        FakeConsumerProtocol $protocol,
        FakeConsumerWorker $worker,
    ): CoroutineConsumerStream {
        return new CoroutineConsumerStream(
            '/tmp/test.sock',
            $transport,
            'cluster',
            'group',
            ['topic'],
            byteCredit: 1024,
            protocol: $protocol,
            worker: $worker,
        );
    }

    /**
     * @return array<string,mixed>
     */
    private function ready(int $maxBytes = 1024): array
    {
        return [
            'kind' => 'ready',
            'subscription_id' => 7,
            'epoch' => 1,
            'resume_token' => 'resume-token',
            'max_in_flight_messages' => 1,
            'max_in_flight_bytes' => $maxBytes,
        ];
    }

    /**
     * @return array<string,mixed>
     */
    private function message(int $deliveryId, int $wireSize): array
    {
        return [
            'kind' => 'message',
            'record' => [
                'subscription_id' => 7,
                'epoch' => 1,
                'delivery_id' => $deliveryId,
                'wire_size' => $wireSize,
            ],
        ];
    }
}

final class FakeConsumerWorker implements ConsumerWorkerInterface
{
    public int $ensureCalls = 0;

    public function ensure(string $socket): void
    {
        ++$this->ensureCalls;
    }
}

final class FakeConsumerTransport implements ConsumerTransportInterface
{
    /**
     * @var list<string>
     */
    public array $sent = [];
    public int $connectCalls = 0;
    private bool $connected = false;

    public function __construct(private string $incoming)
    {
    }

    public function append(string $bytes): void
    {
        $this->incoming .= $bytes;
    }

    public function connect(string $socket, int $timeoutMs): void
    {
        ++$this->connectCalls;
        $this->connected = true;
    }

    public function sendAll(string $data, int $timeoutMs): void
    {
        if (! $this->connected) {
            throw new ConsumerTransportException('not connected');
        }
        $this->sent[] = $data;
    }

    public function receive(int $maxBytes, int $timeoutMs): string
    {
        if ('' === $this->incoming) {
            throw new ConsumerTransportException('idle timeout', timeout: true);
        }
        $chunk = \substr($this->incoming, 0, $maxBytes);
        $this->incoming = (string) \substr($this->incoming, \strlen($chunk));
        return $chunk;
    }

    public function close(): void
    {
        $this->connected = false;
    }

    public function sleep(int $milliseconds): void
    {
    }
}

final class FakeConsumerProtocol implements ConsumerProtocolInterface
{
    private const HEADER_LENGTH = 20;

    public function wire(array $event, int $cid = 0, int $kind = 1): string
    {
        $payload = \json_encode($event, \JSON_THROW_ON_ERROR);
        return \sprintf('%02d%08d%010d', $kind, $cid, \strlen($payload)) . $payload;
    }

    public function headerLength(): int
    {
        return self::HEADER_LENGTH;
    }
    public function errorFrameKind(): int
    {
        return 99;
    }
    public function helloFrame(): string
    {
        return 'HELLO';
    }
    public function verifyHello(string $frame): void
    {
    }

    public function parseHeader(string $header): array
    {
        return [
            'kind' => (int) \substr($header, 0, 2),
            'cid' => (int) \substr($header, 2, 8),
            'payload_len' => (int) \substr($header, 10, 10),
        ];
    }

    public function decodeStreamFrame(string $frame): array
    {
        $event = \json_decode(\substr($frame, self::HEADER_LENGTH), true, flags: \JSON_THROW_ON_ERROR);
        if (($event['kind'] ?? null) === 'message') {
            $record = $event['record'];
            $event['message'] = new ConsumerRecord(
                $record['subscription_id'],
                $record['epoch'],
                $record['delivery_id'],
                'topic',
                0,
                1,
                0,
                $record['wire_size'],
                'key',
                'value',
            );
        }
        return $event;
    }

    public function decodeErrorFrame(string $frame): array
    {
        return ['kind' => 1, 'kind_name' => 'TEST', 'retryable' => false, 'outcome' => 'not_applied', 'native_code' => 0, 'message' => 'test'];
    }

    public function open(string $cluster, string $groupId, array $topics, array $config, int $maxMessages, int $maxBytes): ConsumerRequest
    {
        return new ConsumerRequest(10, 'OPEN');
    }

    public function resume(int $subscriptionId, int $epoch, string $token): ConsumerRequest
    {
        return new ConsumerRequest(11, "RESUME:{$subscriptionId}:{$epoch}");
    }

    public function ack(int $subscriptionId, int $epoch, int $deliveryId, int $messageCredit, int $byteCredit): string
    {
        return "ACK:{$subscriptionId}:{$epoch}:{$deliveryId}:{$messageCredit}:{$byteCredit}";
    }

    public function nack(int $subscriptionId, int $epoch, int $deliveryId, ?string $reason): string
    {
        return "NACK:{$subscriptionId}:{$epoch}:{$deliveryId}";
    }

    public function flow(int $subscriptionId, int $epoch, int $messageCredit, int $byteCredit): string
    {
        return "FLOW:{$subscriptionId}:{$epoch}:{$messageCredit}:{$byteCredit}";
    }

    public function commit(int $subscriptionId, int $epoch): ConsumerRequest
    {
        return new ConsumerRequest(12, "COMMIT:{$subscriptionId}:{$epoch}");
    }

    public function close(int $subscriptionId, int $epoch): string
    {
        return "CLOSE:{$subscriptionId}:{$epoch}";
    }
    public function pause(int $subscriptionId, int $epoch, array $partitions): string
    {
        return "PAUSE:{$subscriptionId}:{$epoch}";
    }
    public function resumePartitions(int $subscriptionId, int $epoch, array $partitions): string
    {
        return "PARTITION_RESUME:{$subscriptionId}:{$epoch}";
    }
}
