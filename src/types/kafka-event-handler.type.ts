import { EachMessagePayload } from "kafkajs";
import { BaseKafkaEvent } from "../events/base.event";

export type KafkaEventHandler<T = Record<string, unknown>> = (
    event: BaseKafkaEvent<T>,
    payload: EachMessagePayload,
) => Promise<void> | void;

/**
 * Narrows an incoming `event.payload` to the shape a handler expects.
 * Required whenever a handler is registered for a payload type other than
 * `Record<string, unknown>`, so the narrowing is backed by a real runtime check.
 */
export type KafkaPayloadValidator<T> = (payload: unknown) => payload is T;