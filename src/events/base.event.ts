export interface BaseKafkaEvent<T = Record<string, unknown>> {
  eventId: string;
  eventType: string;
  timestamp: string;
  source: string;
  traceId?: string;
  payload: T;
}
