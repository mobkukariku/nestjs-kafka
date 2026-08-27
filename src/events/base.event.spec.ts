import { isBaseKafkaEvent } from './base.event';

const valid = {
  eventId: 'e-1',
  eventType: 'PAYMENT_PROCESSED',
  timestamp: '2026-08-27T00:00:00.000Z',
  source: 'billing',
  payload: { orderId: 'o-1' },
};

describe('isBaseKafkaEvent', () => {
  it('принимает полный конверт', () => {
    expect(isBaseKafkaEvent({ ...valid, version: 2 })).toBe(true);
  });

  // Регрессия: раньше version сравнивался со строкой 'undefined', из-за чего
  // каждое событие без этого поля целиком уезжало в DLQ.
  it('принимает конверт без поля version', () => {
    expect(isBaseKafkaEvent(valid)).toBe(true);
  });

  it('принимает явный version: undefined', () => {
    expect(isBaseKafkaEvent({ ...valid, version: undefined })).toBe(true);
  });

  it('отвергает нечисловой version', () => {
    expect(isBaseKafkaEvent({ ...valid, version: '2' })).toBe(false);
  });

  it.each([
    ['eventId', { ...valid, eventId: 1 }],
    ['eventType', { ...valid, eventType: null }],
    ['timestamp', { ...valid, timestamp: 0 }],
    ['source', { ...valid, source: undefined }],
  ])('отвергает конверт с битым полем %s', (_field, event) => {
    expect(isBaseKafkaEvent(event)).toBe(false);
  });

  it('отвергает конверт без payload', () => {
    const { payload, ...rest } = valid;
    expect(isBaseKafkaEvent(rest)).toBe(false);
  });

  it('принимает payload: null — проверяется наличие ключа, не содержимое', () => {
    expect(isBaseKafkaEvent({ ...valid, payload: null })).toBe(true);
  });

  it.each([[null], [undefined], ['строка'], [42], [[]]])('отвергает не-объект %p', (value) => {
    expect(isBaseKafkaEvent(value)).toBe(Array.isArray(value) ? false : false);
  });
});
