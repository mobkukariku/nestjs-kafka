export interface BaseKafkaEvent<T = Record<string, unknown>> {
  eventId: string;
  eventType: string;
  timestamp: string;
  source: string;
  traceId?: string;
  payload: T;
}

export function isBaseKafkaEvent(value: unknown): value is BaseKafkaEvent {
  if (typeof value !== 'object' || value === null) return false;

  const event = value as Record<string, unknown>;

  return (
    typeof event.eventId === 'string' &&
    typeof event.eventType === 'string' &&
    typeof event.timestamp === 'string' &&
    typeof event.source === 'string' &&
    'payload' in event
  );
}
