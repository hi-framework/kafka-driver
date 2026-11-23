<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Hi\Kafka\Internal\AbstractCoroutineClient;

/**
 * Swoole 协程感知的 Kafka 客户端。
 *
 * 所有 UDS IO 走 `Swoole\Coroutine\Socket`，多协程并发共享本进程的 RPC 连接池；
 * Consumer 始终使用不入池的独占 stream connection。协议编解码复用扩展暴露的
 * `hi_kafka_*` 函数，协议逻辑单源。
 *
 * worker 崩溃或被其它进程重启后自动恢复：重新探测/拉起 worker，并重放本进程
 * 注册过的集群。fork 后子进程会自动丢弃继承的连接。
 *
 * 仅在 Swoole 协程上下文中使用。PHP-FPM / CLI / 非协程 Swoole 用 `Client`。
 *
 * ```php
 * Swoole\Coroutine\run(function () {
 *     $client = new Hi\Kafka\SwooleClient('/tmp/hi-kafka-v2.sock');
 *     $client->registerCluster('default', ['bootstrap.servers' => '127.0.0.1:9094']);
 *     $client->produceFnf('default', 'topic', 'k', 'v');
 *     $r = $client->produceSync('default', 'topic', 'k', 'v', timeoutMs: 5000);
 * });
 * ```
 */
final class SwooleClient extends AbstractCoroutineClient
{
    /**
     * @param string $socket         Worker UDS 路径
     * @param int    $maxIdle        空闲连接上限，归还时超出直接关闭
     * @param float  $connectTimeout 建链超时（秒）
     */
    public function __construct(
        string $socket = '/tmp/hi-kafka-v2.sock',
        int $maxIdle = 16,
        float $connectTimeout = 1.0,
    ) {
        self::assertExtension('swoole');
        parent::__construct($socket, $maxIdle, (int) \ceil($connectTimeout * 1000));
    }

    protected function createTransport(): ConsumerTransportInterface
    {
        return new SwooleConsumerTransport;
    }
}
