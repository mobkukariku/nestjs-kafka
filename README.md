# @mobkukariku/nestjs-kafka

Продакшн-готовая NestJS-интеграция с Kafka: продюсер, несколько независимых consumer-групп,
ретраи с бэкоффом, dead letter queue, дедупликация по `eventId` и health-проба —
подключается в любом сервисе через `KafkaModule.forRoot()` / `forRootAsync()`.

Обновляетесь с 1.x? См. [CHANGELOG.md](./CHANGELOG.md) — в 2.0 есть ломающие изменения.

## Установка

```bash
npm install @mobkukariku/nestjs-kafka
```

Требует `.npmrc` с доступом к GitHub Packages в потребляющем репозитории:

```
@mobkukariku:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NPM_TOKEN}
```

Peer-зависимости: `@nestjs/common`, `@nestjs/core`, `kafkajs`, `reflect-metadata`.
`ioredis` — опциональная, нужна только для `RedisDedupeStore`. Node.js >= 20.

## Быстрый старт

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

В `main.ts` **обязательно** включите shutdown-хуки — без них продюсер не закроется
корректно при остановке сервиса:

```ts
const app = await NestFactory.create(AppModule);
app.enableShutdownHooks();
await app.listen(3000);
```

По умолчанию модуль регистрируется глобально. Отключается через `isGlobal: false` —
тогда `KafkaModule` нужно импортировать в каждом модуле, которому он нужен.

## Продюсер

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

`version` проставляется в `1`, если не задан явно, — согласованно в теле сообщения и в
заголовке. Продюсер по умолчанию идемпотентный, `acks: -1`, сжатие GZIP; всё это
переопределяется через `producer` в опциях модуля.

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

## Консьюмеры

Поддерживается несколько независимых consumer-групп (свой `groupId` и набор топиков
у каждой):

```ts
KafkaModule.forRoot({
  clientId: 'billing-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  consumers: [
    {
      groupId: 'billing-service.payments',
      topics: ['billing.payments'],
      deadLetterTopic: 'billing.payments.dlq', // обязателен
      maxRetries: 3,          // попыток на сообщение перед DLQ (по умолчанию 3)
      maxRetryDelayMs: 5000,  // потолок паузы между попытками (по умолчанию 5000)
      partitionsConsumedConcurrently: 1,
    },
  ],
})
```

`deadLetterTopic` обязателен: без него сообщения, которые нельзя обработать, пришлось бы
молча терять.

### Регистрация обработчиков

Обработчик привязан к тройке **топик + тип события + версия конверта**, а не к одному
`eventType` — иначе одинаково названные события из разных топиков перехватывали бы
обработчики друг друга.

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { KafkaConsumerService, BaseKafkaEvent } from '@mobkukariku/nestjs-kafka';

@Injectable()
export class PaymentsListener implements OnModuleInit {
  constructor(private readonly consumer: KafkaConsumerService) {}

  onModuleInit() {
    this.consumer.registerHandler(
      { topic: 'billing.payments', eventType: 'PAYMENT_PROCESSED', version: 1 },
      async (event) => {
        // event.payload: Record<string, unknown>
      },
    );
  }
}
```

Чтение сообщений (`consumer.run()`) стартует не в `onModuleInit`, а в
`onApplicationBootstrap` — так все обработчики приложения гарантированно успевают
зарегистрироваться до прихода первого сообщения. Если группа подписана на топик, для
которого не зарегистрировано ни одного обработчика, приложение падает на старте.

### Типизированный payload

Библиотека проверяет форму конверта `BaseKafkaEvent`, но не содержимое `payload`. Чтобы
получить в обработчике типизированный payload, передайте type guard — сужение типа всегда
подкреплено рантайм-проверкой, а не пустым `as`-кастом:

```ts
interface PaymentProcessedPayload {
  orderId: string;
  amount: number;
}

function isPaymentProcessedPayload(p: unknown): p is PaymentProcessedPayload {
  if (typeof p !== 'object' || p === null) return false;
  const v = p as Record<string, unknown>;
  return typeof v.orderId === 'string' && typeof v.amount === 'number';
}

this.consumer.registerHandler(
  { topic: 'billing.payments', eventType: 'PAYMENT_PROCESSED' },
  async (event) => {
    event.payload.orderId; // string
    event.payload.amount;  // number
  },
  { validate: isPaymentProcessedPayload },
);
```

### Обработка ошибок и DLQ

В `deadLetterTopic` уходит сообщение, если:

| Причина | Ретраится |
|---|---|
| Пустое тело | нет |
| Невалидный JSON | нет |
| Не совпадает форма конверта `BaseKafkaEvent` | нет |
| Нет обработчика для `топик::eventType::версия` | нет |
| `payload` не прошёл валидатор | нет |
| Обработчик упал | да, `maxRetries` раз |

Невалидное сообщение не станет валидным при повторе, поэтому ретраится только падение
обработчика. Паузы между попытками — экспоненциальные с джиттером и потолком
`maxRetryDelayMs`; между попытками отправляется `heartbeat`, чтобы группу не выкинуло
на ребаланс.

В DLQ уходит конверт `DEAD_LETTER` с координатами исходного сообщения (топик, партиция,
оффсет), исходным телом и причиной. **Если публикация в DLQ упала, ошибка пробрасывается
наружу**: kafkajs не закоммитит оффсет и повторит батч — сообщение не потеряется.

## Дедупликация

Каждое событие обрабатывается один раз по `eventId`. Схема трёхфазная: `claim` с коротким
in-flight TTL до обработки, `commit` с основным TTL после успеха, `release` на любом
провальном пути. Короткий TTL на этапе `claim` принципиален — если процесс упадёт между
`claim` и коммитом оффсета, запись протухнет сама и переотданное сообщение будет
обработано, а не отброшено как дубликат.

После ухода в DLQ клейм снимается — иначе ручной реплей из DLQ отбрасывался бы как дубликат.

По умолчанию используется `InMemoryDedupeStore`. **Он per-process**: при нескольких
репликах каждая ведёт свой набор `eventId`, и событие может быть обработано по разу на
каждой реплике. Для мультиреплики нужен Redis:

```bash
npm install ioredis
```

```ts
import Redis from 'ioredis';
import { RedisDedupeStore } from '@mobkukariku/nestjs-kafka/redis';

KafkaModule.forRootAsync({
  useFactory: () => ({
    clientId: 'billing-service',
    brokers: ['localhost:9092'],
    dedupe: new RedisDedupeStore(new Redis(process.env.REDIS_URL!)),
  }),
})
```

Дедупликацию можно отключить для конкретного обработчика: `{ idempotent: false }`.
Свой стор — реализация интерфейса `DedupeStore`.

## Health-проба

```ts
import { Controller, Get } from '@nestjs/common';
import { KafkaHealthIndicator } from '@mobkukariku/nestjs-kafka';

@Controller('health')
export class HealthController {
  constructor(private readonly kafka: KafkaHealthIndicator) {}

  @Get('kafka')
  check() {
    return this.kafka.check();
    // { ready: true, producer: 'connected', consumers: { 'billing-service.payments': 'RUNNING' } }
  }
}
```

`ready` = продюсер подключён (он нужен для DLQ) и все группы в `RUNNING`. Состояния группы:
`INIT` → `RUNNING` → `STOPPED` (штатное гашение) или `CRASHED` (авария). Годится как
readiness-проба в Kubernetes.

## Разработка

```bash
npm run build
npm test
npm run test:cov
```

## Публикация новой версии

```bash
npm run build
npm version patch   # или minor/major
npm publish
```
