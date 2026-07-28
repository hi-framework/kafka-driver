<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

/**
 * @internal worker 生命周期边界
 */
interface ConsumerWorkerInterface
{
    /**
     * 确保指定 UDS 路径对应的 worker 已启动；并发互斥由扩展实现。
     */
    public function ensure(string $socket): void;
}
