<?php

declare(strict_types=1);

namespace Hi\Kafka\Tests;

use Hi\Kafka\ConsumerTransportException;
use Hi\Kafka\ConsumerTransportInterface;
use Hi\Kafka\Internal\ConsumerRequest;
use Hi\Kafka\Internal\CoroutineRpcClient;
use Hi\Kafka\KafkaException;
use PHPUnit\Framework\TestCase;

final class CoroutineRpcClientTest extends TestCase
{
    private FakeConsumerProtocol $protocol;
    private FakeConsumerWorker $worker;
    /** @var list<ScriptedRpcTransport> */
    private array $transports = [];
    /** @var list<list<string>> */
    private array $scripts = [];

    protected function setUp(): void
    {
        $this->protocol = new FakeConsumerProtocol;
        $this->worker = new FakeConsumerWorker;
    }

    public function testReusesIdleConnectionAcrossCalls(): void
    {
        $this->scripts = [[$this->hello(), $this->ok(1), $this->ok(2)]];
        $client = $this->client();

        $client->call($this->request(1), 100);
        $client->call($this->request(2), 100);

        self::assertCount(1, $this->transports);
        self::assertSame(['RPC_HELLO', 'REQ:1', 'REQ:2'], $this->transports[0]->sent);
        self::assertSame(1, $client->stats()['created']);
        self::assertSame(1, $this->worker->ensureCalls);
    }

    public function testDeadWorkerIsProbedAndRequestRetriedOnFreshConnection(): void
    {
        $this->scripts = [
            [$this->hello(), $this->ok(1), ScriptedRpcTransport::EOF],
            [$this->hello(), $this->ok(3)],
        ];
        $client = $this->client();
        $client->call($this->request(1), 100);

        $cid = 2;
        $raw = $client->call(function () use (&$cid): ConsumerRequest {
            $request = new ConsumerRequest($cid, "REQ:{$cid}");
            ++$cid;
            return $request;
        }, 100);

        self::assertSame($this->ok(3), $raw);
        self::assertCount(2, $this->transports);
        self::assertSame(2, $this->worker->ensureCalls, 'initial ensure + forced probe after EOF');
        self::assertSame(['RPC_HELLO', 'REQ:3'], $this->transports[1]->sent);
    }

    public function testUnregisteredClusterIsReplayedThenRetried(): void
    {
        $this->scripts = [[$this->hello(), $this->error(1, 5), $this->ok(1)]];
        $client = $this->client();

        self::assertSame($this->ok(1), $client->call($this->request(1), 100));
        self::assertSame(1, $this->worker->replayCalls);
        self::assertCount(1, $this->transports, 'error frames keep the connection reusable');
    }

    public function testRestartByAnotherProcessRecoversConnectionAndRegistration(): void
    {
        $this->scripts = [
            [$this->hello(), $this->ok(1), ScriptedRpcTransport::EOF],
            [$this->hello(), $this->error(1, 5), $this->ok(1)],
        ];
        $client = $this->client();
        $client->call($this->request(1), 100);

        self::assertSame($this->ok(1), $client->call($this->request(1), 100));
        self::assertSame(2, $this->worker->ensureCalls);
        self::assertSame(1, $this->worker->replayCalls);
    }

    public function testTimeoutIsNotRetriedBecauseOutcomeIsUnknown(): void
    {
        $this->scripts = [[$this->hello()]];
        $client = $this->client();

        try {
            $client->call($this->request(1), 100);
            self::fail('timeout must surface');
        } catch (ConsumerTransportException $error) {
            self::assertTrue($error->isTimeout());
        }
        self::assertSame(['RPC_HELLO', 'REQ:1'], $this->transports[0]->sent);
        self::assertSame(1, $this->worker->ensureCalls);
    }

    public function testNonRecoverableWorkerErrorSurfacesOnce(): void
    {
        $this->scripts = [[$this->hello(), $this->error(1, 1), $this->error(1, 1)]];
        $client = $this->client();

        try {
            $client->call($this->request(1), 100);
            self::fail('invalid argument must surface');
        } catch (KafkaException $error) {
            self::assertSame(1, $error->kind);
        }
        self::assertSame(['RPC_HELLO', 'REQ:1'], $this->transports[0]->sent);
        self::assertSame(0, $this->worker->replayCalls);
    }

    public function testForkedChildNeverReusesParentConnections(): void
    {
        if (! \function_exists('pcntl_fork')) {
            self::markTestSkipped('pcntl is required to verify fork ownership');
        }
        $this->scripts = [[$this->hello(), $this->ok(1)]];
        $client = $this->client();
        $client->call($this->request(1), 100);
        self::assertSame(1, $client->stats()['idle']);

        $pid = \pcntl_fork();
        if (0 === $pid) {
            $stats = $client->stats();
            exit(0 === $stats['idle'] && 0 === $stats['created'] ? 0 : 1);
        }
        \pcntl_waitpid($pid, $status);

        self::assertSame(0, \pcntl_wexitstatus($status));
        self::assertSame(1, $client->stats()['idle'], 'parent keeps its own pool');
    }

    private function client(): CoroutineRpcClient
    {
        return new CoroutineRpcClient(
            '/tmp/test.sock',
            function (): ConsumerTransportInterface {
                $transport = new ScriptedRpcTransport($this->scripts[\count($this->transports)] ?? []);
                $this->transports[] = $transport;
                return $transport;
            },
            4,
            100,
            $this->protocol,
            $this->worker,
        );
    }

    /**
     * @return \Closure(): ConsumerRequest
     */
    private function request(int $cid): \Closure
    {
        return static fn (): ConsumerRequest => new ConsumerRequest($cid, "REQ:{$cid}");
    }

    private function hello(): string
    {
        return $this->protocol->wire(['kind' => 'hello']);
    }

    private function ok(int $cid): string
    {
        return $this->protocol->wire(['ok' => true], cid: $cid);
    }

    private function error(int $cid, int $kind): string
    {
        return $this->protocol->wire([
            'message' => 'worker error',
            'kind' => $kind,
            'kind_name' => 'TEST',
            'retryable' => false,
            'outcome' => 'not_applied',
            'native_code' => 0,
        ], cid: $cid, kind: 99);
    }
}

/**
 * 每次 receive 返回脚本中的下一个完整帧；EOF 标记模拟 worker 关闭连接，
 * 脚本耗尽模拟读超时。
 */
final class ScriptedRpcTransport implements ConsumerTransportInterface
{
    public const EOF = "\0EOF";

    /** @var list<string> */
    public array $sent = [];

    /**
     * @param list<string> $frames
     */
    public function __construct(private array $frames)
    {
    }

    public function connect(string $socket, int $timeoutMs): void
    {
    }

    public function sendAll(string $data, int $timeoutMs): void
    {
        $this->sent[] = $data;
    }

    public function receive(int $maxBytes, int $timeoutMs): string
    {
        $frame = \array_shift($this->frames);
        if (null === $frame) {
            throw new ConsumerTransportException('read timed out', timeout: true);
        }
        if (self::EOF === $frame) {
            throw new ConsumerTransportException('consumer transport reached EOF');
        }
        return $frame;
    }

    public function close(): void
    {
    }

    public function sleep(int $milliseconds): void
    {
    }
}
