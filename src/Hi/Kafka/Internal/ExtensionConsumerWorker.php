<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * 通过 ext-kafka 启动或确认 worker 的生命周期适配器。
 *
 * @internal
 */
final class ExtensionConsumerWorker implements ConsumerWorkerInterface
{
    /**
     * 委托扩展按 socket namespace 执行带进程互斥的 worker ensure。
     */
    public function ensure(string $socket): void
    {
        \hi_kafka_ensure_worker($socket);
    }
}
