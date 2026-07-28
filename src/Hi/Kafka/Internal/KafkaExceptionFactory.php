<?php

declare(strict_types=1);

namespace Hi\Kafka\Internal;

use Hi\Kafka\KafkaException;

/**
 * @internal 确保原生客户端与协程客户端使用完全一致的错误元数据映射
 */
final class KafkaExceptionFactory
{
    /**
     * 把扩展解码后的错误字段映射为稳定的 KafkaException API。
     *
     * @param array{kind:int,kind_name:string,retryable:bool,outcome:string,native_code:int,message:string} $value
     */
    public static function fromDecoded(array $value): KafkaException
    {
        // 使用 Exception 的常规构造函数，让 PHP 正常记录文件、行号与调用栈；
        // 扩展特有的错误分类再复制到稳定的公开 API 属性中。
        $error = new KafkaException((string) $value['message'], (int) $value['kind']);
        $error->kind = (int) $value['kind'];
        $error->kind_name = (string) $value['kind_name'];
        $error->retryable = (bool) $value['retryable'];
        $error->outcome = (string) $value['outcome'];
        $error->native_code = (int) $value['native_code'];

        return $error;
    }
}
