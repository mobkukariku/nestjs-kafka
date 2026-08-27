import { EachMessagePayload } from 'kafkajs';
import { BaseKafkaEvent } from '../events/base.event';

export type KafkaEventHandler<T = Record<string, unknown>> = (
  event: BaseKafkaEvent<T>,
  payload: EachMessagePayload,
) => Promise<void> | void;

export type KafkaPayloadValidator<T> = (payload: unknown) => payload is T;

export interface KafkaHandlerDescriptor {
  topic: string;
  eventType: string;
  /** Версия конверта события. По умолчанию 1. */
  version?: number;
}

export interface RegisterHandlerOptions<T> {
  /**
   * Type guard на payload. Без него payload остаётся `Record<string, unknown>`:
   * сужение типа всегда подкреплено рантайм-проверкой, а не пустым `as`-кастом.
   */
  validate?: KafkaPayloadValidator<T>;
  /** Пропускать повторы по eventId через DedupeStore. По умолчанию true. */
  idempotent?: boolean;
}
