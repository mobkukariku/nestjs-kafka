# Changelog

## 2.0.0

Мажор: исправлены потери сообщений и приведён в порядок публичный контракт.
Требуется правка кода при обновлении с 1.x — см. «Миграция» ниже.

### Исправлено (сообщения больше не теряются)

- **`InMemoryDedupeStore.release()` бросал `Method not implemented`.** Этот стор стоит
  дефолтом, поэтому на штатной конфигурации любое сообщение, провалившее все попытки,
  вылетало из `eachMessage` мимо DLQ. Оффсет не коммитился, kafkajs переотдавал батч,
  `claim()` возвращал false — и сообщение отбрасывалось как дубликат. Итог: не в DLQ и
  не обработано.
- **Дыра at-most-once в дедупликации.** Клейм ставился до обработки и снимался только
  при исчерпании ретраев. Падение процесса или ребаланс между `claim` и коммитом оффсета
  приводили к потере сообщения. Теперь клейм ставится с коротким in-flight TTL, снимается
  в `finally` на любом провальном пути и продлевается через `commit()` только после успеха.
- **`isBaseKafkaEvent` сравнивал `version` со строкой `'undefined'`.** Любое событие без
  поля `version` не проходило проверку конверта и целиком уезжало в DLQ.
- **Бэкофф ретраев мог пережить `sessionTimeout`.** При `maxRetries: 10` пауза внутри
  `eachMessage` доходила до ~100с при дефолтном `sessionTimeout` 30с, и группу выкидывало
  на ребаланс. Добавлен потолок `maxRetryDelayMs` (по умолчанию 5000мс) и `heartbeat()`
  между попытками.
- **Пакет не собирался**: две ошибки TypeScript в `kafka-consumer.service.ts`.
- **Продюсер отключался слишком рано.** `disconnect()` переехал из `onModuleDestroy` в
  `onApplicationShutdown`, который Nest вызывает строго позже остановки консьюмеров, —
  иначе публикация в DLQ из недообработанного батча падала на закрытом соединении.
- **`STOP` помечал группу `CRASHED`**, из-за чего health рапортовал аварию на каждом
  штатном гашении. Добавлено состояние `STOPPED`.

### Добавлено

- `KafkaModuleOptions.producer` — `idempotent`, `acks`, `compression`,
  `maxInFlightRequests`, `allowAutoTopicCreation` вместо зашитых констант.
- `KafkaConsumerOptions.partitionsConsumedConcurrently` и `maxRetryDelayMs`.
- `forRoot({ isGlobal })` — модуль больше не навязывает глобальную область принудительно
  (по умолчанию по-прежнему `true`).
- Тесты (jest) и CI на GitHub Actions.

### Миграция 1.x → 2.0

1. **`deadLetterTopic` стал обязательным** в `KafkaConsumerOptions`. Раньше поле было
   опциональным в типе, но конструктор всё равно падал без него, а JSDoc и README
   обещали «логируется и отбрасывается» — три разных ответа. Укажите топик явно:
   ```ts
   consumers: [{ groupId, topics, deadLetterTopic: 'billing.payments.dlq' }]
   ```
2. **`DedupeStore` получил третий метод `commit()`.** Если у вас своя реализация:
   ```ts
   interface DedupeStore {
     claim(eventId: string): Promise<boolean>;   // короткий in-flight TTL
     commit(eventId: string): Promise<void>;     // основной TTL после успеха
     release(eventId: string): Promise<void>;    // снять клейм
   }
   ```
3. **`RedisDedupStore` → `RedisDedupeStore`** и переехал в отдельную точку входа.
   `ioredis` теперь optional peer dependency — установите его сами, если нужен Redis:
   ```ts
   // было: import { RedisDedupStore } from '@mobkukariku/nestjs-kafka';
   import { RedisDedupeStore } from '@mobkukariku/nestjs-kafka/redis';
   ```
   Старое имя оставлено как `@deprecated` алиас и будет удалено в 3.0.
4. **Добавьте `app.enableShutdownHooks()`** в `main.ts` — без него продюсер не закроется
   корректно.
5. Требуется Node.js >= 20.

## 1.1.0

- Типобезопасное сужение payload в обработчиках консьюмера.

## 1.0.0

- Kafka-консьюмер с несколькими группами, ретраями и DLQ поверх общего клиента.
