// nestjs-kafka/src/kafka-consumer.service.ts
import {
  Injectable, Inject, Logger,
  OnModuleInit, OnApplicationBootstrap, OnModuleDestroy,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { KAFKA_CLIENT, MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
import {
  KafkaEventHandler, KafkaPayloadValidator,
  KafkaHandlerDescriptor, RegisterHandlerOptions,
} from './types/kafka-event-handler.type';
import { KafkaConsumerOptions, KafkaModuleOptions } from './types/kafka-module-options.interface';
import { BaseKafkaEvent, isBaseKafkaEvent } from './events/base.event';
import { KafkaProducerService } from './kafka-producer.service';
import { DEDUPE_STORE, type DedupeStore } from './dedupe/dedupe.store';

const DEFAULT_MAX_ATTEMPTS = 3;
const BASE_RETRY_DELAY_MS = 200;
const DEFAULT_MAX_RETRY_DELAY_MS = 5_000;

export type ConsumerState = 'INIT' | 'RUNNING' | 'STOPPED' | 'CRASHED';

interface ManagedConsumer {
  consumer: Consumer;
  options: KafkaConsumerOptions;
  state: ConsumerState;
}

interface RegisteredHandler {
  descriptor: Required<KafkaHandlerDescriptor>;
  handler: KafkaEventHandler;
  validate?: KafkaPayloadValidator<unknown>;
  idempotent: boolean;
}

export interface DeadLetterPayload {
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  raw: string;
  reason: string;
}

/** Ключ роутинга включает топик и версию, а не только eventType. */
function routingKey(topic: string, eventType: string, version: number): string {
  return `${topic}::${eventType}::v${version}`;
}

@Injectable()
export class KafkaConsumerService
  implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(KafkaConsumerService.name);
  private readonly managed = new Map<string, ManagedConsumer>();
  private readonly handlers = new Map<string, RegisteredHandler>();

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    @Inject(MODULE_OPTIONS_TOKEN) private readonly options: KafkaModuleOptions,
    private readonly producer: KafkaProducerService,
    @Inject(DEDUPE_STORE) private readonly dedupe: DedupeStore,
  ) {
    for (const cfg of this.options.consumers ?? []) {
      if (this.managed.has(cfg.groupId)) {
        throw new Error(`Duplicate Kafka consumer groupId "${cfg.groupId}"`);
      }

      const consumer = this.kafka.consumer({
        groupId: cfg.groupId,
        sessionTimeout: cfg.sessionTimeout,
        rebalanceTimeout: cfg.rebalanceTimeout,
      });

      const managed: ManagedConsumer = { consumer, options: cfg, state: 'INIT' };

      // Состояние группы — то, что отдаёт readiness-проба.
      consumer.on(consumer.events.GROUP_JOIN, () => {
        managed.state = 'RUNNING';
        this.logger.log(`Consumer "${cfg.groupId}" joined group`);
      });
      consumer.on(consumer.events.CRASH, (e) => {
        managed.state = 'CRASHED';
        this.logger.error(
          `Consumer "${cfg.groupId}" crashed (restart=${e.payload.restart})`,
          e.payload.error,
        );
      });
      // STOP — это и штатное гашение тоже, поэтому не CRASHED: иначе health
      // рапортует аварию на каждом graceful shutdown.
      consumer.on(consumer.events.STOP, () => {
        if (managed.state !== 'CRASHED') managed.state = 'STOPPED';
      });

      this.managed.set(cfg.groupId, managed);
    }
  }

  /** Типизированный payload: сужение подкреплено рантайм-проверкой. */
  registerHandler<T>(
    descriptor: KafkaHandlerDescriptor,
    handler: KafkaEventHandler<T>,
    options: RegisterHandlerOptions<T> & { validate: KafkaPayloadValidator<T> },
  ): void;
  /** Без валидатора payload остаётся `Record<string, unknown>`. */
  registerHandler(
    descriptor: KafkaHandlerDescriptor,
    handler: KafkaEventHandler,
    options?: Omit<RegisterHandlerOptions<never>, 'validate'>,
  ): void;
  registerHandler<T>(
    descriptor: KafkaHandlerDescriptor,
    handler: KafkaEventHandler<T>,
    options: RegisterHandlerOptions<T> = {},
  ): void {
    const full = { ...descriptor, version: descriptor.version ?? 1 };
    const key = routingKey(full.topic, full.eventType, full.version);

    // Раньше вторая регистрация молча затирала первую.
    if (this.handlers.has(key)) {
      throw new Error(`Обработчик для ${key} уже зарегистрирован`);
    }

    this.handlers.set(key, {
      descriptor: full,
      handler: handler as KafkaEventHandler,
      validate: options.validate as KafkaPayloadValidator<unknown> | undefined,
      idempotent: options.idempotent ?? true,
    });
  }

  /** Готов = все группы в RUNNING и продюсер подключён (нужен для DLQ). */
  isReady(): boolean {
    return (
      this.producer.isConnected() &&
      [...this.managed.values()].every((m) => m.state === 'RUNNING')
    );
  }

  getState(): Record<string, ConsumerState> {
    return Object.fromEntries(
      [...this.managed.values()].map((m) => [m.options.groupId, m.state]),
    );
  }

  async onModuleInit(): Promise<void> {
    if (this.managed.size === 0) {
      this.logger.warn('Консьюмеры не сконфигурированы');
      return;
    }
    // Без try/catch: не подключились — Nest не поднимется, процесс выйдет.
    for (const { consumer, options } of this.managed.values()) {
      await consumer.connect();
      await consumer.subscribe({ topics: options.topics });
    }
  }

  async onApplicationBootstrap(): Promise<void> {
    this.assertEveryTopicHandled();

    for (const m of this.managed.values()) {
      await m.consumer.run({
        partitionsConsumedConcurrently: m.options.partitionsConsumedConcurrently,
        eachMessage: (payload) => this.handleMessage(payload, m.options),
      });
      this.logger.log(`Consumer "${m.options.groupId}" running`);
    }
  }

  /**
   * Консьюмеры гасятся здесь, продюсер — в onApplicationShutdown, который Nest
   * вызывает строго позже. Так публикации в DLQ из недообработанных батчей
   * гарантированно успевают пройти по живому продюсеру.
   */
  async onModuleDestroy(): Promise<void> {
    for (const { consumer, options } of this.managed.values()) {
      try {
        await consumer.disconnect();
        this.logger.log(`Consumer "${options.groupId}" disconnected`);
      } catch (error) {
        this.logger.error(`Disconnect failed for "${options.groupId}"`, error);
      }
    }
  }

  /** Подписались на топик, но ни одного обработчика — почти наверняка баг конфигурации. */
  private assertEveryTopicHandled(): void {
    const handled = new Set([...this.handlers.values()].map((h) => h.descriptor.topic));

    for (const m of this.managed.values()) {
      for (const topic of m.options.topics) {
        if (!handled.has(topic)) {
          throw new Error(
            `Группа "${m.options.groupId}" подписана на "${topic}" без единого обработчика`,
          );
        }
      }
    }
  }

  private async handleMessage(
    msg: EachMessagePayload,
    cfg: KafkaConsumerOptions,
  ): Promise<void> {
    const raw = msg.message.value?.toString();
    if (!raw) {
      await this.toDeadLetter(msg, cfg, '', 'Пустое тело сообщения');
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      await this.toDeadLetter(msg, cfg, raw, 'Невалидный JSON');
      return;
    }

    if (!isBaseKafkaEvent(parsed)) {
      await this.toDeadLetter(msg, cfg, raw, 'Не совпадает форма конверта');
      return;
    }

    const event = parsed;
    const key = routingKey(msg.topic, event.eventType, event.version ?? 1);
    const registered = this.handlers.get(key);

    if (!registered) {
      // Раньше это был warn и потерянное событие.
      await this.toDeadLetter(msg, cfg, raw, `Нет обработчика для ${key}`);
      return;
    }

    if (registered.validate && !registered.validate(event.payload)) {
      await this.toDeadLetter(msg, cfg, raw, `Payload не прошёл валидацию для ${key}`);
      return;
    }

    if (!registered.idempotent) {
      await this.runWithRetries(registered, event, msg, cfg, raw);
      return;
    }

    if (!(await this.dedupe.claim(event.eventId))) {
      this.logger.debug(`Дубликат eventId=${event.eventId} (${key}), пропускаем`);
      return;
    }

    let succeeded = false;
    try {
      succeeded = await this.runWithRetries(registered, event, msg, cfg, raw);
    } finally {
      // Клейм снимается на ЛЮБОМ провальном пути, включая падение публикации в
      // DLQ. Иначе переотданное сообщение будет молча отброшено как дубликат —
      // и потеряно. По той же причине release нужен после ухода в DLQ: без него
      // ручной реплей из DLQ не пройдёт.
      await this.finalizeClaim(event.eventId, succeeded);
    }
  }

  /** Ошибка стора не должна маскировать исходную ошибку обработки. */
  private async finalizeClaim(eventId: string, succeeded: boolean): Promise<void> {
    try {
      if (succeeded) await this.dedupe.commit(eventId);
      else await this.dedupe.release(eventId);
    } catch (error) {
      this.logger.error(
        `DedupeStore.${succeeded ? 'commit' : 'release'} упал для eventId=${eventId}`,
        error,
      );
    }
  }

  /** @returns true, если хендлер отработал успешно. false — сообщение ушло в DLQ. */
  private async runWithRetries(
    reg: RegisteredHandler,
    event: BaseKafkaEvent,
    msg: EachMessagePayload,
    cfg: KafkaConsumerOptions,
    raw: string,
  ): Promise<boolean> {
    const maxAttempts = cfg.maxRetries ?? DEFAULT_MAX_ATTEMPTS;
    let lastError: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await reg.handler(event, msg);
        return true;
      } catch (error) {
        lastError = error;
        this.logger.error(
          `Handler "${event.eventType}" упал ` +
            `(попытка ${attempt}/${maxAttempts}, traceId=${event.traceId ?? '-'})`,
          error,
        );
        if (attempt < maxAttempts) {
          await this.sleep(this.backoff(attempt, cfg));
          // Пауза съедает бюджет sessionTimeout — без heartbeat группу выкинет
          // на ребаланс прямо посреди ретраев.
          await msg.heartbeat();
        }
      }
    }

    const reason = lastError instanceof Error ? lastError.message : String(lastError);
    await this.toDeadLetter(msg, cfg, raw, `Исчерпаны ${maxAttempts} попыток: ${reason}`);
    return false;
  }

  private async toDeadLetter(
    msg: EachMessagePayload,
    cfg: KafkaConsumerOptions,
    raw: string,
    reason: string,
  ): Promise<void> {
    const dlqEvent: BaseKafkaEvent<DeadLetterPayload> = {
      eventId: randomUUID(),
      eventType: 'DEAD_LETTER',
      version: 1,
      timestamp: new Date().toISOString(),
      source: cfg.groupId,
      payload: {
        originalTopic: msg.topic,
        originalPartition: msg.partition,
        originalOffset: msg.message.offset,
        raw,
        reason,
      },
    };

    try {
      await this.producer.publish(
        cfg.deadLetterTopic,
        msg.message.key?.toString() ?? msg.message.offset,
        dlqEvent,
      );
      this.logger.warn(`→ DLQ "${cfg.deadLetterTopic}": ${reason}`);
    } catch (error) {
      // Не смогли записать в DLQ — бросаем наружу. kafkajs не закоммитит оффсет
      // и повторит батч. Сообщение не потеряется.
      this.logger.error(`Не удалось записать в DLQ "${cfg.deadLetterTopic}"`, error);
      throw error;
    }
  }

  /** Экспоненциальный бэкофф с джиттером и потолком — иначе ретраи переживают sessionTimeout. */
  private backoff(attempt: number, cfg: KafkaConsumerOptions): number {
    const cap = cfg.maxRetryDelayMs ?? DEFAULT_MAX_RETRY_DELAY_MS;
    const exp = Math.min(BASE_RETRY_DELAY_MS * 2 ** (attempt - 1), cap);
    return Math.round(exp * (0.5 + Math.random() * 0.5));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
