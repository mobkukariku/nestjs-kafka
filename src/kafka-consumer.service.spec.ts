import { Logger } from '@nestjs/common';
import { EachMessagePayload, Kafka } from 'kafkajs';
import { KafkaConsumerService, DeadLetterPayload } from './kafka-consumer.service';
import { KafkaProducerService } from './kafka-producer.service';
import {
  KafkaConsumerOptions,
  KafkaModuleOptions,
} from './types/kafka-module-options.interface';
import { InMemoryDedupeStore } from './dedupe/in-memory-dedupe.store';
import { DedupeStore } from './dedupe/dedupe.store';
import { BaseKafkaEvent } from './events/base.event';

const TOPIC = 'billing.payments';
const DLQ = 'billing.payments.dlq';
const GROUP = 'billing.payments.group';

type EachMessage = (payload: EachMessagePayload) => Promise<void>;

function fakeConsumer() {
  const listeners: Record<string, (e: never) => void> = {};
  let eachMessage: EachMessage | undefined;

  return {
    events: { GROUP_JOIN: 'GROUP_JOIN', CRASH: 'CRASH', STOP: 'STOP' },
    on: jest.fn((event: string, cb: (e: never) => void) => { listeners[event] = cb; }),
    connect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn(async (cfg: { eachMessage: EachMessage }) => { eachMessage = cfg.eachMessage; }),
    disconnect: jest.fn().mockResolvedValue(undefined),
    emit: (event: string, e?: unknown) => listeners[event]?.(e as never),
    deliver: (payload: EachMessagePayload) => eachMessage!(payload),
  };
}

interface BuildOverrides {
  consumer?: Partial<KafkaConsumerOptions>;
  dedupe?: DedupeStore;
  publish?: jest.Mock;
}

function build(overrides: BuildOverrides = {}) {
  const consumer = fakeConsumer();
  const kafka = { consumer: jest.fn(() => consumer) } as unknown as Kafka;
  const publish = overrides.publish ?? jest.fn().mockResolvedValue(undefined);
  const producer = { isConnected: () => true, publish } as unknown as KafkaProducerService;
  const dedupe = overrides.dedupe ?? new InMemoryDedupeStore();

  const options: KafkaModuleOptions = {
    clientId: 'test',
    brokers: ['localhost:9092'],
    consumers: [
      {
        groupId: GROUP,
        topics: [TOPIC],
        deadLetterTopic: DLQ,
        maxRetries: 3,
        maxRetryDelayMs: 1,
        ...overrides.consumer,
      },
    ],
  };

  const service = new KafkaConsumerService(kafka, options, producer, dedupe);
  return { service, consumer, publish, dedupe };
}

function message(body: unknown, opts: { raw?: string | null } = {}): EachMessagePayload {
  const serialized = 'raw' in opts ? opts.raw : JSON.stringify(body);
  return {
    topic: TOPIC,
    partition: 0,
    message: {
      key: Buffer.from('order-1'),
      value: serialized === null ? null : Buffer.from(serialized as string),
      offset: '42',
      timestamp: '0',
      attributes: 0,
      headers: {},
    },
    heartbeat: jest.fn().mockResolvedValue(undefined),
    pause: jest.fn(() => () => undefined),
  } as unknown as EachMessagePayload;
}

function event(overrides: Partial<BaseKafkaEvent> = {}): BaseKafkaEvent {
  return {
    eventId: 'e-1',
    eventType: 'PAYMENT_PROCESSED',
    version: 1,
    timestamp: '2026-08-27T00:00:00.000Z',
    source: 'billing',
    payload: { orderId: 'o-1' },
    ...overrides,
  };
}

function dlqReason(publish: jest.Mock, call = 0): string {
  const [topic, , dlqEvent] = publish.mock.calls[call];
  expect(topic).toBe(DLQ);
  return (dlqEvent as BaseKafkaEvent<DeadLetterPayload>).payload.reason;
}

/** Поднимает сервис до состояния «читаем сообщения» и возвращает харнесс. */
async function started(
  overrides: BuildOverrides = {},
  handler: jest.Mock = jest.fn(),
  registerOptions: { idempotent?: boolean } = {},
) {
  const harness = build(overrides);
  await harness.service.onModuleInit();
  harness.service.registerHandler(
    { topic: TOPIC, eventType: 'PAYMENT_PROCESSED' },
    handler,
    registerOptions,
  );
  await harness.service.onApplicationBootstrap();
  return { ...harness, handler };
}

beforeAll(() => {
  for (const level of ['log', 'warn', 'error', 'debug'] as const) {
    jest.spyOn(Logger.prototype, level).mockImplementation(() => undefined);
  }
});

describe('KafkaConsumerService — конфигурация', () => {
  it('запрещает дублирующийся groupId', () => {
    const kafka = { consumer: jest.fn(() => fakeConsumer()) } as unknown as Kafka;
    const options: KafkaModuleOptions = {
      clientId: 'test',
      brokers: [],
      consumers: [
        { groupId: GROUP, topics: [TOPIC], deadLetterTopic: DLQ },
        { groupId: GROUP, topics: ['other'], deadLetterTopic: DLQ },
      ],
    };
    expect(
      () =>
        new KafkaConsumerService(
          kafka,
          options,
          { isConnected: () => true } as KafkaProducerService,
          new InMemoryDedupeStore(),
        ),
    ).toThrow(/Duplicate Kafka consumer groupId/);
  });

  it('запрещает повторную регистрацию одного и того же ключа роутинга', async () => {
    const { service } = build();
    service.registerHandler({ topic: TOPIC, eventType: 'X' }, jest.fn());
    expect(() => service.registerHandler({ topic: TOPIC, eventType: 'X' }, jest.fn()))
      .toThrow(/уже зарегистрирован/);
  });

  it('различает обработчики по версии конверта', () => {
    const { service } = build();
    service.registerHandler({ topic: TOPIC, eventType: 'X', version: 1 }, jest.fn());
    expect(() =>
      service.registerHandler({ topic: TOPIC, eventType: 'X', version: 2 }, jest.fn()),
    ).not.toThrow();
  });

  it('падает на старте, если топик подписан без обработчиков', async () => {
    const { service } = build();
    await service.onModuleInit();
    await expect(service.onApplicationBootstrap()).rejects.toThrow(/без единого обработчика/);
  });

  it('прокидывает partitionsConsumedConcurrently в consumer.run', async () => {
    const { consumer } = await started({ consumer: { partitionsConsumedConcurrently: 4 } });
    expect(consumer.run).toHaveBeenCalledWith(
      expect.objectContaining({ partitionsConsumedConcurrently: 4 }),
    );
  });
});

describe('KafkaConsumerService — маршрутизация в DLQ', () => {
  it('пустое тело → DLQ', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message(null, { raw: null }));
    expect(dlqReason(publish)).toBe('Пустое тело сообщения');
  });

  it('битый JSON → DLQ', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message(null, { raw: '{не json' }));
    expect(dlqReason(publish)).toBe('Невалидный JSON');
  });

  it('чужая форма конверта → DLQ', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message({ hello: 'world' }));
    expect(dlqReason(publish)).toBe('Не совпадает форма конверта');
  });

  it('нет обработчика → DLQ, а не потерянное событие', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message(event({ eventType: 'SYSTEM_PING' })));
    expect(dlqReason(publish)).toMatch(/Нет обработчика/);
  });

  it('несовпадение версии → DLQ', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message(event({ version: 7 })));
    expect(dlqReason(publish)).toMatch(/v7/);
  });

  it('провал валидатора → DLQ без единого вызова хендлера', async () => {
    const handler = jest.fn();
    const harness = build();
    await harness.service.onModuleInit();
    harness.service.registerHandler(
      { topic: TOPIC, eventType: 'PAYMENT_PROCESSED' },
      handler,
      { validate: (p): p is { orderId: string } => false },
    );
    await harness.service.onApplicationBootstrap();

    await harness.consumer.deliver(message(event()));

    expect(handler).not.toHaveBeenCalled();
    expect(dlqReason(harness.publish)).toMatch(/не прошёл валидацию/);
  });

  it('DLQ-конверт несёт координаты исходного сообщения', async () => {
    const { consumer, publish } = await started();
    await consumer.deliver(message({ hello: 'world' }));

    const dlqEvent = publish.mock.calls[0][2] as BaseKafkaEvent<DeadLetterPayload>;
    expect(dlqEvent.eventType).toBe('DEAD_LETTER');
    expect(dlqEvent.source).toBe(GROUP);
    expect(dlqEvent.payload).toMatchObject({
      originalTopic: TOPIC,
      originalPartition: 0,
      originalOffset: '42',
      raw: JSON.stringify({ hello: 'world' }),
    });
  });

  it('падение публикации в DLQ пробрасывается наружу — оффсет не коммитится', async () => {
    const publish = jest.fn().mockRejectedValue(new Error('брокер недоступен'));
    const { consumer } = await started({ publish });
    await expect(consumer.deliver(message(null, { raw: '{не json' })))
      .rejects.toThrow('брокер недоступен');
  });
});

describe('KafkaConsumerService — ретраи', () => {
  it('успех с первой попытки — один вызов, без DLQ', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { consumer, publish } = await started({}, handler);

    await consumer.deliver(message(event()));

    expect(handler).toHaveBeenCalledTimes(1);
    expect(publish).not.toHaveBeenCalled();
  });

  it('успех после падения — повтор без DLQ', async () => {
    const handler = jest.fn()
      .mockRejectedValueOnce(new Error('таймаут'))
      .mockResolvedValue(undefined);
    const { consumer, publish } = await started({}, handler);

    await consumer.deliver(message(event()));

    expect(handler).toHaveBeenCalledTimes(2);
    expect(publish).not.toHaveBeenCalled();
  });

  it('исчерпание попыток → maxRetries вызовов и DLQ', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('база лежит'));
    const { consumer, publish } = await started({ consumer: { maxRetries: 3 } }, handler);

    await consumer.deliver(message(event()));

    expect(handler).toHaveBeenCalledTimes(3);
    expect(dlqReason(publish)).toBe('Исчерпаны 3 попыток: база лежит');
  });

  it('шлёт heartbeat между попытками, иначе группу выкинет на ребаланс', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('нет'));
    const { consumer } = await started({ consumer: { maxRetries: 3 } }, handler);
    const msg = message(event());

    await consumer.deliver(msg);

    expect(msg.heartbeat).toHaveBeenCalledTimes(2); // между 3 попытками — 2 паузы
  });

  it('держит потолок паузы независимо от номера попытки', async () => {
    const delays: number[] = [];
    const realSetTimeout = global.setTimeout;
    jest.spyOn(global, 'setTimeout').mockImplementation(((cb: () => void, ms?: number) => {
      delays.push(ms ?? 0);
      return realSetTimeout(cb, 0);
    }) as typeof setTimeout);

    const handler = jest.fn().mockRejectedValue(new Error('нет'));
    const { consumer } = await started(
      { consumer: { maxRetries: 8, maxRetryDelayMs: 500 } },
      handler,
    );
    await consumer.deliver(message(event()));

    expect(delays).toHaveLength(7);
    for (const d of delays) expect(d).toBeLessThanOrEqual(500);
    jest.mocked(global.setTimeout).mockRestore();
  });
});

describe('KafkaConsumerService — дедупликация', () => {
  it('повторный eventId пропускается', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { consumer } = await started({}, handler);

    await consumer.deliver(message(event()));
    await consumer.deliver(message(event()));

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('idempotent: false отключает дедупликацию', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const { consumer } = await started({}, handler, { idempotent: false });

    await consumer.deliver(message(event()));
    await consumer.deliver(message(event()));

    expect(handler).toHaveBeenCalledTimes(2);
  });

  // Регрессия 2.0.0: раньше InMemoryDedupeStore.release() бросал
  // "Method not implemented", исключение уходило из eachMessage мимо DLQ,
  // и на переотдаче сообщение отбрасывалось как дубликат — то есть терялось.
  it('падение всех попыток на дефолтном сторе доводит сообщение до DLQ', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('база лежит'));
    const { consumer, publish } = await started({}, handler);

    await expect(consumer.deliver(message(event()))).resolves.toBeUndefined();

    expect(publish).toHaveBeenCalledTimes(1);
    expect(dlqReason(publish)).toMatch(/Исчерпаны/);
  });

  it('после ухода в DLQ клейм снят — реплей из DLQ проходит', async () => {
    const handler = jest.fn().mockRejectedValueOnce(new Error('раз'))
      .mockRejectedValueOnce(new Error('два'))
      .mockRejectedValueOnce(new Error('три'))
      .mockResolvedValue(undefined);
    const { consumer, dedupe } = await started({ consumer: { maxRetries: 3 } }, handler);

    await consumer.deliver(message(event()));
    await expect(dedupe.claim('e-1')).resolves.toBe(true);
  });

  it('клейм снимается и когда публикация в DLQ упала', async () => {
    const publish = jest.fn().mockRejectedValue(new Error('брокер недоступен'));
    const handler = jest.fn().mockRejectedValue(new Error('база лежит'));
    const { consumer, dedupe } = await started({ publish }, handler);

    await expect(consumer.deliver(message(event()))).rejects.toThrow('брокер недоступен');

    // Иначе переотданное сообщение было бы молча отброшено как дубликат.
    await expect(dedupe.claim('e-1')).resolves.toBe(true);
  });

  it('успешная обработка фиксируется через commit', async () => {
    const dedupe: DedupeStore = {
      claim: jest.fn().mockResolvedValue(true),
      commit: jest.fn().mockResolvedValue(undefined),
      release: jest.fn().mockResolvedValue(undefined),
    };
    const { consumer } = await started({ dedupe }, jest.fn().mockResolvedValue(undefined));

    await consumer.deliver(message(event()));

    expect(dedupe.commit).toHaveBeenCalledWith('e-1');
    expect(dedupe.release).not.toHaveBeenCalled();
  });

  it('ошибка стора не маскирует исходную ошибку обработки', async () => {
    const dedupe: DedupeStore = {
      claim: jest.fn().mockResolvedValue(true),
      commit: jest.fn().mockResolvedValue(undefined),
      release: jest.fn().mockRejectedValue(new Error('redis лежит')),
    };
    const publish = jest.fn().mockRejectedValue(new Error('брокер недоступен'));
    const handler = jest.fn().mockRejectedValue(new Error('база лежит'));
    const { consumer } = await started({ dedupe, publish }, handler);

    await expect(consumer.deliver(message(event())))
      .rejects.toThrow('брокер недоступен');
  });
});

describe('KafkaConsumerService — состояние и здоровье', () => {
  it('до входа в группу — INIT и не готов', async () => {
    const { service } = build();
    expect(service.getState()).toEqual({ [GROUP]: 'INIT' });
    expect(service.isReady()).toBe(false);
  });

  it('GROUP_JOIN переводит в RUNNING и делает готовым', async () => {
    const { service, consumer } = build();
    consumer.emit('GROUP_JOIN');
    expect(service.isReady()).toBe(true);
  });

  it('CRASH переводит в CRASHED', async () => {
    const { service, consumer } = build();
    consumer.emit('GROUP_JOIN');
    consumer.emit('CRASH', { payload: { restart: false, error: new Error('boom') } });
    expect(service.getState()).toEqual({ [GROUP]: 'CRASHED' });
    expect(service.isReady()).toBe(false);
  });

  // Регрессия: STOP помечал группу CRASHED, из-за чего health рапортовал
  // аварию на каждом штатном гашении.
  it('STOP переводит в STOPPED, а не в CRASHED', async () => {
    const { service, consumer } = build();
    consumer.emit('GROUP_JOIN');
    consumer.emit('STOP');
    expect(service.getState()).toEqual({ [GROUP]: 'STOPPED' });
  });

  it('STOP после CRASH не затирает диагноз', async () => {
    const { service, consumer } = build();
    consumer.emit('CRASH', { payload: { restart: false, error: new Error('boom') } });
    consumer.emit('STOP');
    expect(service.getState()).toEqual({ [GROUP]: 'CRASHED' });
  });

  it('onModuleDestroy отключает консьюмеров', async () => {
    const { service, consumer } = build();
    await service.onModuleDestroy();
    expect(consumer.disconnect).toHaveBeenCalled();
  });
});
