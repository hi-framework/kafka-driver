<?php

declare(strict_types=1);

namespace Hi\Kafka;

if (! \interface_exists(ClientInterface::class, false)) {
    /**
     * Kafka driver 的统一客户端契约。
     *
     * 原生阻塞客户端、Swoole 客户端与 Swow 客户端均实现此接口；协议编解码和
     * worker 所有权由 ext-kafka 提供，具体实现只负责适配各自运行时的 IO 模型。
     */
    interface ClientInterface
    {
        /**
         * 在 worker 中注册或覆盖一个具名 Kafka 集群配置。
         *
         * @param array<string,string> $config librdkafka 配置，必须包含 `bootstrap.servers`
         */
        public function registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void;

        /**
         * 确保当前 socket namespace 对应的 hi-kafka worker 已启动并可接受连接。
         */
        public function ensureWorker(): void;

        /**
         * 将消息提交到 worker 的本地生产队列，不等待 broker delivery report。
         *
         * worker 可同步识别的错误仍会抛出；方法成功只表示 worker 已接收请求。
         *
         * @param array<string,string>|null $headers Kafka 消息头
         */
        public function produceFnf(string $cluster, string $topic, string $key, string $value, ?array $headers = null, ?int $partition = null, ?int $timestampMs = null): void;

        /**
         * 生产一条消息并等待 broker delivery report。
         *
         * @param array<string,string>|null $headers Kafka 消息头
         *
         * @return array<string,mixed> broker 确认结果，包含 partition、offset 或错误信息
         */
        public function produceSync(string $cluster, string $topic, string $key, string $value, ?array $headers = null, ?int $partition = null, ?int $timestampMs = null, ?int $timeoutMs = null): array;

        /**
         * 创建独占连接的单消息 Consumer stream。
         *
         * @param list<string>              $topics 订阅主题列表
         * @param array<string,string>|null $config 当前 subscription 的 librdkafka 配置
         */
        public function consume(string $cluster, string $groupId, array $topics, ?array $config = null): ConsumerStreamInterface;

        /**
         * 向 worker 更新指定集群的 SASL/OAUTHBEARER token。
         *
         * @param array<string,string>|null $extensions OAUTHBEARER 扩展字段
         */
        public function setOAuthBearerToken(string $cluster, string $token, int $lifetimeMs, string $principalName, ?array $extensions = null, ?int $timeoutMs = null): void;
    }
}
