import {
  Injectable,
  Inject,
  Logger,
  OnModuleInit,
  OnApplicationShutdown,
} from '@nestjs/common';
import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { KAFKA_CLIENT, MODULE_OPTIONS_TOKEN } from './kafka.module-definition';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';
import { BaseKafkaEvent } from './events/base.event';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly producer: Producer;
  private readonly acks: number;
  private readonly compression: CompressionTypes;
  private connected = false;

  constructor(
    @Inject(KAFKA_CLIENT)
    private readonly kafka: Kafka,
    @Inject(MODULE_OPTIONS_TOKEN)
    private readonly options: KafkaModuleOptions,
  ) {
    const cfg = this.options.producer ?? {};
    const idempotent = cfg.idempotent ?? true;

    this.acks = cfg.acks ?? -1;
    this.compression = cfg.compression ?? CompressionTypes.GZIP;

    this.producer = this.kafka.producer({
      idempotent,
      // kafkajs требует ровно 1 при idempotent: true.
      maxInFlightRequests: idempotent ? 1 : cfg.maxInFlightRequests,
      ...(cfg.allowAutoTopicCreation !== undefined && {
        allowAutoTopicCreation: cfg.allowAutoTopicCreation,
      }),
    });

    if (idempotent && cfg.maxInFlightRequests !== undefined && cfg.maxInFlightRequests !== 1) {
      this.logger.warn(
        `maxInFlightRequests=${cfg.maxInFlightRequests} игнорируется: ` +
          'идемпотентный продюсер kafkajs допускает только 1.',
      );
    }

    this.producer.on(this.producer.events.CONNECT, () => { this.connected = true; });
    this.producer.on(this.producer.events.DISCONNECT, () => { this.connected = false; });
  }

  isConnected(): boolean {
    return this.connected;
  }

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.connected = true;
    this.logger.log('Kafka producer connected');
  }

  /**
   * Не onModuleDestroy: продюсер должен пережить остановку консьюмеров, иначе
   * публикация в DLQ из недообработанного батча упадёт на закрытом соединении.
   * Требует app.enableShutdownHooks().
   */
  async onApplicationShutdown(): Promise<void> {
    try {
      await this.producer.disconnect();
      this.logger.log('Kafka Producer disconnected gracefully!');
    } catch (error) {
      this.logger.error('Producer disconnect failed', error);
    }
  }

  async publish<T>(
    topic: string,
    key: string,
    event: BaseKafkaEvent<T>,
  ): Promise<void> {
    const version = event.version ?? 1;

    try {
      await this.producer.send({
        topic,
        acks: this.acks,
        compression: this.compression,
        messages: [
          {
            key,
            // version в конце: явный undefined в событии не должен затирать дефолт,
            // а тело обязано совпасть с заголовком.
            value: JSON.stringify({ ...event, version }),
            headers: {
              eventId: event.eventId,
              source: event.source,
              version: String(version),
              timestamp: event.timestamp,
              ...(event.traceId && { traceId: event.traceId }),
            },
          },
        ],
      });

      this.logger.debug(
        `[Kafka] Event "${event.eventType}" -> Topic "${topic}" (Key: ${key})`,
      );
    } catch (error) {
      this.logger.error(`Failed to publish event to topic "${topic}"`, error);
      throw error;
    }
  }
}
