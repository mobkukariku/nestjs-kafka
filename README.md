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

## Публикация новой версии

```bash
npm run build
npm version patch   # или minor/major
npm publish
```

## Consumer

Пока пакет поддерживает только producer. `KafkaModuleOptions.consumer` зарезервирован под будущий `KafkaConsumerService` — добавится без breaking changes для существующих потребителей.
