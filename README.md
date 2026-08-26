# @mobkukariku/nestjs-kafka

Переиспользуемая NestJS Kafka-интеграция: подключается в любом сервисе через `KafkaModule.forRoot()`/`forRootAsync()` без копирования boilerplate-кода.

## Установка

```bash
npm install @mobkukariku/nestjs-kafka
```

Требует `.npmrc` с доступом к GitHub Packages в потребляющем репозитории:

```
@mobkukariku:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NPM_TOKEN}
```

## Использование

```ts
import { Module } from '@nestjs/common';
import { KafkaModule } from '@mobkukariku/nestjs-kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'billing-service',
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    }),
  ],
})
export class AppModule {}
```

```ts
import { Injectable } from '@nestjs/common';
import { KafkaProducerService, BaseKafkaEvent } from '@mobkukariku/nestjs-kafka';

@Injectable()
export class BillingService {
  constructor(private readonly kafka: KafkaProducerService) {}

  async processPayment(orderId: string, amount: number) {
    const event: BaseKafkaEvent<{ orderId: string; amount: number }> = {
      eventId: crypto.randomUUID(),
      eventType: 'PAYMENT_PROCESSED',
      timestamp: new Date().toISOString(),
      source: 'billing-service',
      payload: { orderId, amount },
    };

    await this.kafka.publish('billing.payments', orderId, event);
  }
}
```

## Асинхронная конфигурация

```ts
KafkaModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: async (config: ConfigService) => ({
    clientId: config.get('KAFKA_CLIENT_ID'),
    brokers: config.get('KAFKA_BROKERS').split(','),
  }),
})
```

## Consumer

Модуль поддерживает несколько независимых consumer'ов (свой `groupId` и набор топиков у каждого) через `KafkaModuleOptions.consumers`:

```ts
KafkaModule.forRoot({
  clientId: 'billing-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  consumers: [
    {
      groupId: 'billing-service.payments',
      topics: ['billing.payments'],
      maxRetries: 3,               // попыток на сообщение перед DLQ (по умолчанию 3)
      deadLetterTopic: 'billing.payments.dlq', // опционально
    },
    {
      groupId: 'billing-service.refunds',
      topics: ['billing.refunds'],
    },
  ],
})
```

Хендлеры регистрируются через `KafkaConsumerService.registerHandler(eventType, handler)` — обычно в `onModuleInit()` своего сервиса:

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { KafkaConsumerService, BaseKafkaEvent } from '@mobkukariku/nestjs-kafka';

@Injectable()
export class PaymentsListener implements OnModuleInit {
  constructor(private readonly consumer: KafkaConsumerService) {}

  onModuleInit() {
    this.consumer.registerHandler('PAYMENT_PROCESSED', async (event: BaseKafkaEvent) => {
      // обработка события
    });
  }
}
```

Реальное чтение сообщений (`consumer.run()`) стартует не в `onModuleInit`, а в `onApplicationBootstrap` — это гарантирует, что хендлеры из всех модулей приложения успеют зарегистрироваться до прихода первого сообщения.

**Обработка ошибок:** входящее сообщение валидируется на соответствие форме `BaseKafkaEvent` (`eventId`, `eventType`, `timestamp`, `source`, `payload`). Если сообщение невалидно — оно сразу уходит в `deadLetterTopic` (если задан) или логируется и отбрасывается. Если хендлер бросает ошибку — попытка повторяется до `maxRetries` раз с нарастающей паузой, после чего сообщение также уходит в `deadLetterTopic` (через `KafkaProducerService`) либо логируется и отбрасывается, если `deadLetterTopic` не задан.

## Публикация новой версии

```bash
npm run build
npm version patch   # или minor/major
npm publish
```
