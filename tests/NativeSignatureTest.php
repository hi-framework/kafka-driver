<?php

declare(strict_types=1);

namespace Hi\Kafka\Tests;

use Hi\Kafka\Client;
use Hi\Kafka\ConsumerRecord;
use Hi\Kafka\ConsumerStream;
use PHPUnit\Framework\TestCase;

/**
 * 原生类必须在反射中暴露可选参数的默认值，否则跳过中间可选参数的命名参数调用
 * （例如 `produceSync(..., timeoutMs: 1000)`）会抛 ArgumentCountError，
 * 与 ClientInterface / ConsumerStreamInterface 的签名不一致。
 */
final class NativeSignatureTest extends TestCase
{
    protected function setUp(): void
    {
        if (! \extension_loaded('hi-kafka')) {
            self::markTestSkipped('requires the native hi-kafka extension');
        }
    }

    public function testEveryOptionalNativeParameterDeclaresItsDefault(): void
    {
        $missing = [];
        foreach ([Client::class, ConsumerStream::class, ConsumerRecord::class] as $class) {
            foreach ((new \ReflectionClass($class))->getMethods() as $method) {
                foreach ($method->getParameters() as $parameter) {
                    if ($parameter->isOptional() && ! $parameter->isDefaultValueAvailable()) {
                        $missing[] = "{$class}::{$method->getName()}(\${$parameter->getName()})";
                    }
                }
            }
        }

        self::assertSame([], $missing);
    }

    public function testNamedArgumentsMaySkipIntermediateOptionals(): void
    {
        $record = new ConsumerRecord(
            subscriptionId: 1,
            subscriptionEpoch: 2,
            deliveryId: 3,
            topic: 'orders',
            partition: 4,
            offset: 5,
            timestampMs: 6,
            wireSize: 7,
            key: null,
            value: "\0binary",
        );

        self::assertNull($record->key());
        self::assertSame("\0binary", $record->value());
        self::assertSame([], $record->headers());
    }
}
