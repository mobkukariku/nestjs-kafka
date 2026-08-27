import { CompressionTypes, SASLOptions } from 'kafkajs';
import { ConnectionOptions as TLSOptions } from 'tls';
import { DedupeStore } from '../dedupe/dedupe.store';

export interface KafkaConsumerOptions {
  groupId: string;
  topics: string[];
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  /** Сколько раз вызвать хендлер, прежде чем отправить сообщение в DLQ. По умолчанию 3. */
  maxRetries?: number;
  /**
   * Потолок паузы между попытками, мс. По умолчанию 5000.
   * Без потолка экспоненциальный бэкофф при большом maxRetries переживает
   * sessionTimeout, и группу выкидывает на ребаланс.
   */
  maxRetryDelayMs?: number;
  /**
   * Топик, куда уходит сообщение при исчерпании ретраев, невалидной форме,
   * провале валидатора payload или отсутствии обработчика. Обязателен: без него
   * такие сообщения пришлось бы молча терять.
   */
  deadLetterTopic: string;
  /** Сколько партиций обрабатывать параллельно. По умолчанию 1 (kafkajs). */
  partitionsConsumedConcurrently?: number;
}

export interface KafkaProducerOptions {
  /** Идемпотентный продюсер (защита от дублей при внутренних ретраях). По умолчанию true. */
  idempotent?: boolean;
  /**
   * По умолчанию 1. При idempotent: true kafkajs требует ровно 1 —
   * значение больше единицы игнорируется с предупреждением.
   */
  maxInFlightRequests?: number;
  /** По умолчанию -1 (все ISR). */
  acks?: number;
  /** По умолчанию CompressionTypes.GZIP. */
  compression?: CompressionTypes;
  allowAutoTopicCreation?: boolean;
}

export interface KafkaModuleOptions {
  clientId: string;
  brokers: string[];
  logLevel?: 'NOTHING' | 'ERROR' | 'WARN' | 'INFO' | 'DEBUG';
  retry?: {
    initialRetryTime?: number;
    retries?: number;
  };
  ssl?: boolean | TLSOptions;
  sasl?: SASLOptions;
  consumers?: KafkaConsumerOptions[];
  producer?: KafkaProducerOptions;
  /**
   * Стор дедупликации. По умолчанию InMemoryDedupeStore — он per-process
   * и не даёт гарантии при нескольких репликах.
   */
  dedupe?: DedupeStore;
}
