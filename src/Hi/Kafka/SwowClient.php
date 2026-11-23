<?php

declare(strict_types=1);

namespace Hi\Kafka;

use Hi\Kafka\Internal\AbstractCoroutineClient;

/**
 * Swow 协程感知的 Kafka 客户端。
 *
 * 与 {@see SwooleClient} 共用同一份连接池、恢复与 Consumer 实现，只替换为
 * `Swow\Socket` adapter；Swow 原生微秒超时只在 adapter 内换算。
 *
 * ```php
 * Swow\Coroutine::run(function () {
 *     $client = new Hi\Kafka\SwowClient('/tmp/hi-kafka-v2.sock');
 *     $client->registerCluster('default', ['bootstrap.servers' => '127.0.0.1:9094']);
 *     $client->produceFnf('default', 'topic', 'k', 'v');
 * });
 * ```
 */
final class SwowClient extends AbstractCoroutineClient
{
    /**
     * @param string $socket           Worker UDS 路径
     * @param int    $maxIdle          空闲连接上限，归还时超出直接关闭
     * @param int    $connectTimeoutMs 建链超时（毫秒）
     */
    public function __construct(
        string $socket = '/tmp/hi-kafka-v2.sock',
        int $maxIdle = 16,
        int $connectTimeoutMs = 1000,
    ) {
        self::assertExtension('swow');
        parent::__construct($socket, $maxIdle, $connectTimeoutMs);
    }

    protected function createTransport(): ConsumerTransportInterface
    {
        return new SwowConsumerTransport;
    }
}
