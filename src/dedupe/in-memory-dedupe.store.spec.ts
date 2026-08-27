import { InMemoryDedupeStore } from './in-memory-dedupe.store';

describe('InMemoryDedupeStore', () => {
  it('отдаёт клейм один раз', async () => {
    const store = new InMemoryDedupeStore();
    await expect(store.claim('e-1')).resolves.toBe(true);
    await expect(store.claim('e-1')).resolves.toBe(false);
  });

  // Регрессия: release() был `throw new Error("Method not implemented.")`,
  // и на дефолтном сторе упавшее сообщение терялось вместо ухода в DLQ.
  it('release снимает клейм и не бросает', async () => {
    const store = new InMemoryDedupeStore();
    await store.claim('e-1');
    await expect(store.release('e-1')).resolves.toBeUndefined();
    await expect(store.claim('e-1')).resolves.toBe(true);
  });

  it('commit удерживает клейм после основного TTL-продления', async () => {
    const store = new InMemoryDedupeStore();
    await store.claim('e-1');
    await store.commit('e-1');
    await expect(store.claim('e-1')).resolves.toBe(false);
  });

  it('release после commit снова открывает событие (реплей из DLQ)', async () => {
    const store = new InMemoryDedupeStore();
    await store.claim('e-1');
    await store.commit('e-1');
    await store.release('e-1');
    await expect(store.claim('e-1')).resolves.toBe(true);
  });

  it('протухший in-flight клейм не блокирует переобработку', async () => {
    jest.useFakeTimers().setSystemTime(0);
    // ttl 24ч, maxSize по умолчанию, in-flight TTL 1000мс
    const store = new InMemoryDedupeStore(24 * 3600_000, 100, 1_000);

    await expect(store.claim('e-1')).resolves.toBe(true);
    jest.setSystemTime(1_500);
    // Именно это спасает сообщение, если процесс упал между claim и коммитом оффсета.
    await expect(store.claim('e-1')).resolves.toBe(true);

    jest.useRealTimers();
  });

  it('commit ставит длинный TTL, переживающий in-flight окно', async () => {
    jest.useFakeTimers().setSystemTime(0);
    const store = new InMemoryDedupeStore(10_000, 100, 1_000);

    await store.claim('e-1');
    await store.commit('e-1');
    jest.setSystemTime(5_000);
    await expect(store.claim('e-1')).resolves.toBe(false);

    jest.useRealTimers();
  });

  it('вытесняет протухшие записи, не давая карте расти', async () => {
    jest.useFakeTimers().setSystemTime(0);
    const store = new InMemoryDedupeStore(1_000, 100, 1_000);

    for (let i = 0; i < 50; i++) await store.claim(`e-${i}`);
    expect(store.size()).toBe(50);

    jest.setSystemTime(2_000);
    await store.claim('trigger');
    expect(store.size()).toBe(1);

    jest.useRealTimers();
  });

  it('удерживает maxSize и предупреждает при вытеснении живых записей', async () => {
    const warn = jest.spyOn(require('@nestjs/common').Logger.prototype, 'warn')
      .mockImplementation(() => undefined);
    const store = new InMemoryDedupeStore(24 * 3600_000, 5);

    for (let i = 0; i < 20; i++) await store.claim(`e-${i}`);

    expect(store.size()).toBeLessThanOrEqual(5);
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});
