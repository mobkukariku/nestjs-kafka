import type { Redis } from 'ioredis';
import { RedisDedupeStore, RedisDedupStore } from './redis-dedupe.store';

function fakeRedis() {
  return {
    set: jest.fn().mockResolvedValue('OK'),
    del: jest.fn().mockResolvedValue(1),
  };
}

describe('RedisDedupeStore', () => {
  it('claim ставит короткий in-flight TTL через SET NX', async () => {
    const redis = fakeRedis();
    const store = new RedisDedupeStore(redis as unknown as Redis, 3600, 'evt:', 60);

    await expect(store.claim('e-1')).resolves.toBe(true);
    expect(redis.set).toHaveBeenCalledWith('evt:e-1', '1', 'EX', 60, 'NX');
  });

  it('claim возвращает false, когда ключ уже занят', async () => {
    const redis = fakeRedis();
    redis.set.mockResolvedValueOnce(null);
    const store = new RedisDedupeStore(redis as unknown as Redis);
    await expect(store.claim('e-1')).resolves.toBe(false);
  });

  // SET, а не EXPIRE: если in-flight ключ успел протухнуть, EXPIRE по
  // несуществующему ключу ничего не сделает и дубликат пройдёт повторно.
  it('commit пересоздаёт ключ с основным TTL', async () => {
    const redis = fakeRedis();
    const store = new RedisDedupeStore(redis as unknown as Redis, 3600, 'evt:', 60);

    await store.commit('e-1');
    expect(redis.set).toHaveBeenCalledWith('evt:e-1', '1', 'EX', 3600);
  });

  it('release удаляет ключ', async () => {
    const redis = fakeRedis();
    const store = new RedisDedupeStore(redis as unknown as Redis, 3600, 'evt:');
    await store.release('e-1');
    expect(redis.del).toHaveBeenCalledWith('evt:e-1');
  });

  it('уважает кастомный префикс', async () => {
    const redis = fakeRedis();
    const store = new RedisDedupeStore(redis as unknown as Redis, 3600, 'billing:');
    await store.release('e-1');
    expect(redis.del).toHaveBeenCalledWith('billing:e-1');
  });

  it('устаревший алиас указывает на тот же класс', () => {
    expect(RedisDedupStore).toBe(RedisDedupeStore);
  });
});
