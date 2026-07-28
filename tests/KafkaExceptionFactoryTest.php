<?php

declare(strict_types=1);

namespace Hi\Kafka\Tests;

use Hi\Kafka\Internal\KafkaExceptionFactory;
use PHPUnit\Framework\TestCase;

final class KafkaExceptionFactoryTest extends TestCase
{
    public function testDecodedMetadataIsMappedWithoutLosingExceptionCode(): void
    {
        $error = KafkaExceptionFactory::fromDecoded([
            'kind' => 9,
            'kind_name' => 'CREDIT_EXHAUSTED',
            'retryable' => true,
            'outcome' => 'unknown',
            'native_code' => -184,
            'message' => 'queue full',
        ]);

        self::assertSame('queue full', $error->getMessage());
        self::assertSame(9, $error->getCode());
        self::assertSame(9, $error->getKind());
        self::assertSame('CREDIT_EXHAUSTED', $error->getKindName());
        self::assertTrue($error->isRetryable());
        self::assertSame('unknown', $error->getOutcome());
        self::assertSame(-184, $error->getNativeCode());
    }
}
