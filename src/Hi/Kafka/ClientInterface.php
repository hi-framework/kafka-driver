<?php

declare(strict_types=1);

namespace Hi\Kafka;

/*
 * `Hi\Kafka\ClientInterface` 由 hi-kafka 扩展 MINIT 阶段以 **空 marker interface**
 * 形式注册（`ClassFlags::Interface`，无 abstract method）。本文件是仅在扩展缺失时
 * 才会被 PSR-4 autoload 触发的同名桩——给 IDE / 静态分析 / 无扩展 CI 兜底，
 * 让上层代码用 `ClientInterface` 作 type-hint 时静态层能看到完整方法签名。
 *
 * `interface_exists(..., false)` 关掉 autoload 探测自身，避免回环：
 *
 * - 扩展存在 → MINIT 已注册同名 interface（空 body）→ guard 命中 → 不重复定义
 * - 扩展缺失 → autoload 触发 → guard 不命中 → 落桩
 *
 * **为什么扩展端注册的是空 interface 而桩里却含 18 个方法签名**：
 * 扩展端 MINIT 注册空 marker，是为了让 ext 类 `Hi\Kafka\Client` 无需在
 * Rust 侧重复声明抽象方法即可 implements 本接口。三个实现（ext `Client` /
 * `SwooleClient` / `SwowClient`）的 18 个共享方法签名**已完全统一**——
 * camelCase 参数名、参数顺序、`?type = null` 可选形态逐一一致，因此无扩展
 * 环境下三者对本桩接口的 LSP 校验也全部通过（曾经的 `$timeoutMs` 默认值 /
 * `produceSync` 参数顺序差异会在无扩展 CI 触发 "must be compatible" fatal，
 * 现已消除）。静态分析层（PHPStan / Psalm / IDE）只读源码看到桩里的签名，
 * 用于推断业务侧方法调用是否合法。
 *
 * 本接口只声明**三者公共业务方法**（约 18 个）；下列**不**在内：
 * - `Client::__construct(?string $socket)` —— 构造参数三家不同（SwooleClient 有 maxIdle）
 * - `Client::socket(): string` —— 只 Client 有，调试观测用
 * - `Client::produceFnfBin` / `produceSyncBin` —— 只 Client 有，binary-safe 路径
 * - `SwooleClient::stats(): array` / `SwowClient::stats(): array` —— 连接池统计，Client 没有
 *
 * 业务代码用本接口类型时，上述特殊方法走具体子类型断言或 `instanceof` 缩窄。
 */

if (! \interface_exists(ClientInterface::class, false)) {
    /**
     * Kafka 客户端公共接口。三个实现：
     * - {@see Client}       —— 扩展类，阻塞 IO（PHP-FPM / CLI）
     * - {@see SwooleClient} —— 协程感知（Swoole）
     * - {@see SwowClient}   —— 协程感知（Swow）
     *
     * Marker interface：扩展端 MINIT 注册时无 abstract method，方法签名约束仅由
     * 静态分析侧本桩文件提供。
     */
    interface ClientInterface
    {
        // ====================================================================
        // 控制面
        // ====================================================================

        /**
         * 注册或覆盖 Kafka 集群配置。
         *
         * @param array<string,string> $config librdkafka 配置，必须含 `bootstrap.servers`
         */
        public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void;

        /** 显式拉起 worker 进程（命中缓存时零开销）。第一次 produce / subscribe 会自动触发。 */
        public function ensureWorker(): void;

        // ====================================================================
        // Producer
        // ====================================================================

        /**
         * Fire-and-forget 生产。**不等 broker ack**，吞吐最高，无投递成功保证。
         *
         * @param array<string,string> $headers
         */
        public function produceFnf(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null,
        ): void;

        /**
         * 同步生产，等 broker delivery report。
         *
         * @param array<string,string> $headers
         *
         * @return array{
         *     ok: bool,
         *     partition?: int,
         *     offset?: int,
         *     code?: int,
         *     message?: string,
         *     retryable?: bool,
         * }
         */
        public function produceSync(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null,
            ?int $timeoutMs = null,
        ): array;

        // ====================================================================
        // Consumer
        // ====================================================================

        /**
         * 订阅 topics 到 group。
         *
         * @param string[]                  $topics
         * @param array<string,string>|null $config librdkafka consumer 配置
         *
         * @return int virtual subscription_id；worker 崩溃自愈后 id 不变
         */
        public function subscribe(
            string $cluster,
            string $groupId,
            array $topics,
            ?array $config = null,
            ?int $timeoutMs = null,
        ): int;

        /**
         * 拉一批消息。`timeoutMs=0` 非阻塞快照；否则 long-poll。
         *
         * @return list<array{
         *     topic: string,
         *     partition: int,
         *     offset: int,
         *     timestamp_ms: int,
         *     key: string,
         *     value: string,
         *     headers: array<string,string>,
         * }>
         */
        public function poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array;

        /** 同步提交当前持有的 offsets */
        public function commit(int $subscriptionId, ?int $timeoutMs = null): void;

        /** 退订（幂等） */
        public function unsubscribe(int $subscriptionId): void;

        /**
         * 拉取 rebalance 事件队列。
         *
         * @return list<array{type: string, partitions?: list<array{topic: string, partition: int}>, message?: string}>
         */
        public function pollRebalanceEvents(
            int $subscriptionId,
            ?int $maxEvents = null,
            ?int $timeoutMs = null,
        ): array;

        /**
         * 按 offset seek。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets
         */
        public function seek(
            int $subscriptionId,
            array $topics,
            array $partitions,
            array $offsets,
            ?int $timeoutMs = null,
        ): void;

        /**
         * 按时间戳 seek。空 `$topics`/`$partitions` 应用到当前 assignment 全部。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function seekToTimestamp(
            int $subscriptionId,
            int $timestampMs,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null,
        ): void;

        /**
         * 暂停 per-partition fetch。不丢分区分配 / 不触发 rebalance。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function pause(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null,
        ): void;

        /**
         * 恢复被 pause 的分区。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         */
        public function resume(
            int $subscriptionId,
            array $topics,
            array $partitions,
            ?int $timeoutMs = null,
        ): void;

        // ====================================================================
        // 事务 + EOS Stream + SASL/OAUTHBEARER
        // ====================================================================

        /** 开启事务。集群配置必须含 `transactional.id`。 */
        public function beginTransaction(string $cluster, ?int $timeoutMs = null): void;

        /** 原子提交事务里所有 in-flight 消息。 */
        public function commitTransaction(string $cluster, ?int $timeoutMs = null): void;

        /** 回滚事务，read_committed consumer 看不到这些消息。 */
        public function abortTransaction(string $cluster, ?int $timeoutMs = null): void;

        /**
         * EOS Stream (KIP-447)：把 consumer offsets 进当前事务。
         *
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets next offset = last_consumed + 1
         */
        public function sendOffsetsToTransaction(
            string $producerCluster,
            int $subscriptionId,
            string $groupId,
            array $topics,
            array $partitions,
            array $offsets,
            ?int $timeoutMs = null,
        ): void;

        /**
         * 推送 SASL/OAUTHBEARER token。
         *
         * @param array<string,string> $extensions
         */
        public function setOAuthBearerToken(
            string $cluster,
            string $token,
            int $lifetimeMs,
            string $principalName,
            ?array $extensions = null,
            ?int $timeoutMs = null,
        ): void;
    }
}
