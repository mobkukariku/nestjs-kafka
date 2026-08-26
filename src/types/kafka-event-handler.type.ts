import { EachMessagePayload } from "kafkajs";
import { BaseKafkaEvent } from "../events/base.event";

export type KafkaEventHandler<T = Record<string, unknown>> = (
    event: BaseKafkaEvent<T>,
    payload: EachMessagePayload,
) => Promise<void> | void;