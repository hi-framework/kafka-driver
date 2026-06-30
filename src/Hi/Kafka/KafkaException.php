<?php

declare(strict_types=1);

namespace Hi\Kafka;

/*
 * `Hi\Kafka\KafkaException` 由 hi-kafka 扩展 MINIT 阶段以 `ClassBuilder extends \Exception`
 * 注册（手动注册而非 `#[php_class]`——后者会覆盖 `\Exception` 的 `create_object`，导致
 * **堆栈/文件/行号丢失**；详见 `ext/src/kafka_exception.rs`）。带公开属性
 * `kind`/`kind_name`/`retryable`/`native_code` 与 getter `getKind`/`getKindName`/
 * `isRetryable`/`getNativeCode`。本文件是仅在扩展缺失时才被 PSR-4 autoload 触发的同名
 * 桩——给 IDE / 静态分析 / 无扩展 CI 兜底，**形态必须与扩展端一致**，否则装/不装扩展时
 * API 漂移。
 *
 * `class_exists(..., false)` 关掉 autoload 探测自身，避免回环：
 * - 扩展存在 → MINIT 已注册同名类 → guard 命中 → 不重复定义
 * - 扩展缺失 → autoload 触发 → guard 不命中 → 落桩
 *
 * 构造与扩展端一致：沿用（继承的）`\Exception::__construct($message, $code = kind)`（2 参）；
 * 分类信息由抛出方（worker 错误帧 → `ipc_err_to_php` / 协程 driver `makeKafka`）构造后写入。
 */

if (! \class_exists(KafkaException::class, false)) {
    /**
     * 所有 Kafka 操作失败抛出的统一异常。额外携带机器可读的错误分类
     * （见 worker 端 `ErrorKind`：WORKER_UNAVAILABLE / TIMEOUT / BROKER_RETRYABLE /
     * AUTHN_AUTHZ / CLUSTER_NOT_REGISTERED / SUBSCRIPTION_NOT_FOUND / ...）。
     */
    class KafkaException extends \Exception
    {
        public int $kind = 0;
        public string $kind_name = 'INTERNAL';
        public bool $retryable = false;
        public int $native_code = 0;

        /** 机器可读错误大类（数值）。 */
        public function getKind(): int
        {
            return $this->kind;
        }

        /** 错误大类名（如 "BROKER_RETRYABLE"）。 */
        public function getKindName(): string
        {
            return $this->kind_name;
        }

        /** 是否值得重试。 */
        public function isRetryable(): bool
        {
            return $this->retryable;
        }

        /** 原生 librdkafka 错误码（无则 0）。 */
        public function getNativeCode(): int
        {
            return $this->native_code;
        }
    }
}
