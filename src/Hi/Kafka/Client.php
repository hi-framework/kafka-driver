<?php

declare(strict_types=1);

namespace Hi\Kafka;

// 原生扩展会在 MINIT 阶段注册真实类；只有扩展缺失时才加载此静态分析桩。
if (! \class_exists(Client::class, false)) {
    /**
     * 原生阻塞客户端的用户态占位类。
     *
     * 该类只保证 IDE、静态分析及无扩展 CI 能解析公开 API；构造时必然报错，
     * 不能作为可运行的 Kafka 客户端使用。
     */
    final class Client implements ClientInterface
    {
        /**
         * 拒绝在 hi-kafka 扩展未加载时创建原生客户端。
         */
        public function __construct(?string $socket = null)
        {
            throw new \RuntimeException('hi-kafka extension is not loaded');
        }

        /**
         * 描述真实扩展提供的 worker socket getter；占位类不可运行，固定返回空串。
         */
        public function socket(): string
        {
            return '';
        }

        /**
         * 保留真实扩展的集群注册方法签名；因构造必然失败，不会进入此实现。
         */
        public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void
        {
        }

        /**
         * 保留真实扩展的 worker 启动方法签名；因构造必然失败，不会进入此实现。
         */
        public function ensureWorker(): void
        {
        }

        /**
         * 保留真实扩展的即发即弃生产方法签名；因构造必然失败，不会进入此实现。
         */
        public function produceFnf(string $cluster, string $topic, string $key, string $value, ?array $headers = null, ?int $partition = null, ?int $timestampMs = null): void
        {
        }

        /**
         * 保留真实扩展的同步生产方法签名；因构造必然失败，不会进入此实现。
         *
         * @return array<string,mixed>
         */
        public function produceSync(string $cluster, string $topic, string $key, string $value, ?array $headers = null, ?int $partition = null, ?int $timestampMs = null, ?int $timeoutMs = null): array
        {
            return [];
        }

        /**
         * 保留真实扩展的消费方法签名，并在误调用时明确报告扩展缺失。
         */
        public function consume(string $cluster, string $groupId, array $topics, ?array $config = null): ConsumerStreamInterface
        {
            throw new \RuntimeException('hi-kafka extension is not loaded');
        }

        /**
         * 保留真实扩展的 OAUTHBEARER token 更新方法签名；因构造必然失败，不会进入此实现。
         */
        public function setOAuthBearerToken(string $cluster, string $token, int $lifetimeMs, string $principalName, ?array $extensions = null, ?int $timeoutMs = null): void
        {
        }
    }
}
