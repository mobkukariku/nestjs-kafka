import { logLevel } from 'kafkajs';
import { createKafkaClient } from './kafka-client.factory';
import { KafkaModuleOptions } from './types/kafka-module-options.interface';

jest.mock('kafkajs', () => {
  const actual = jest.requireActual('kafkajs');
  return { ...actual, Kafka: jest.fn().mockImplementation((cfg) => ({ cfg })) };
});

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { Kafka } = require('kafkajs') as { Kafka: jest.Mock };

const base: KafkaModuleOptions = { clientId: 'billing', brokers: ['b1:9092'] };

function configOf(options: KafkaModuleOptions) {
  Kafka.mockClear();
  createKafkaClient(options);
  return Kafka.mock.calls[0][0];
}

describe('createKafkaClient', () => {
  it('дефолты: ERROR, 8 ретраев, 300мс', () => {
    expect(configOf(base)).toMatchObject({
      clientId: 'billing',
      brokers: ['b1:9092'],
      logLevel: logLevel.ERROR,
      retry: { initialRetryTime: 300, retries: 8 },
    });
  });

  it('маппит уровень логирования', () => {
    expect(configOf({ ...base, logLevel: 'DEBUG' }).logLevel).toBe(logLevel.DEBUG);
    expect(configOf({ ...base, logLevel: 'NOTHING' }).logLevel).toBe(logLevel.NOTHING);
  });

  it('уважает настройки ретраев', () => {
    expect(configOf({ ...base, retry: { retries: 2 } }).retry)
      .toEqual({ initialRetryTime: 300, retries: 2 });
  });

  it('не передаёт ssl/sasl, если они не заданы', () => {
    const cfg = configOf(base);
    expect(cfg).not.toHaveProperty('ssl');
    expect(cfg).not.toHaveProperty('sasl');
  });

  it('передаёт ssl: false как явное значение', () => {
    expect(configOf({ ...base, ssl: false })).toHaveProperty('ssl', false);
  });

  it('передаёт sasl', () => {
    const sasl = { mechanism: 'plain', username: 'u', password: 'p' } as const;
    expect(configOf({ ...base, sasl }).sasl).toEqual(sasl);
  });
});
