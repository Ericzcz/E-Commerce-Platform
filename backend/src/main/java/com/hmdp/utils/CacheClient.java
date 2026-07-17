package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * Two-level cache client used by shop queries.
 *
 * <p>Only dynamically detected hot entries are promoted to Caffeine. Redis
 * stores a single logical-expiration representation so pass-through and
 * logical-expiration strategies cannot deserialize each other's values.</p>
 */
@Component
public class CacheClient {

    private static final DefaultRedisScript<Long> HOT_KEY_SCRIPT;

    static {
        HOT_KEY_SCRIPT = new DefaultRedisScript<>();
        HOT_KEY_SCRIPT.setLocation(new ClassPathResource("hot-key.lua"));
        HOT_KEY_SCRIPT.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final Cache<String, Object> localCache = Caffeine.newBuilder()
            .maximumSize(1_000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .recordStats()
            .build();
    private final ExecutorService rebuildExecutor = new ThreadPoolExecutor(
            2,
            8,
            60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(200),
            runnable -> {
                Thread thread = new Thread(runnable, "cache-rebuild");
                thread.setDaemon(true);
                return thread;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    private final LongAdder requests = new LongAdder();
    private final LongAdder localHits = new LongAdder();
    private final LongAdder redisHits = new LongAdder();
    private final LongAdder databaseLoads = new LongAdder();
    private final LongAdder rebuilds = new LongAdder();

    public CacheClient(
            StringRedisTemplate stringRedisTemplate,
            @Qualifier("redissonClient") RedissonClient redissonClient) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        long physicalTtlSeconds = Math.max(timeUnit.toSeconds(time) * 2, 60);
        stringRedisTemplate.opsForValue().set(
                key,
                JSONUtil.toJsonStr(redisData),
                physicalTtlSeconds,
                TimeUnit.SECONDS
        );
    }

    /**
     * Compatibility method retained for non-hot callers.
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit timeUnit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }
        R value = dbFallback.apply(id);
        if (value == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        set(key, value, time, timeUnit);
        return value;
    }

    public <R, ID> R queryWithMultiLevelCache(
            String keyPrefix,
            String lockPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long logicalTtl,
            TimeUnit timeUnit) {
        requests.increment();
        String key = keyPrefix + id;

        Object localValue = localCache.getIfPresent(key);
        if (localValue != null) {
            localHits.increment();
            return type.cast(localValue);
        }

        boolean hot = recordAndCheckHot(key);
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            R cached = readCachedValue(json, type);
            LocalDateTime expireTime = readExpireTime(json);
            redisHits.increment();
            if (expireTime == null) {
                // Migrate legacy plain JSON entries to the single logical-expiry format.
                setWithLogicalExpire(key, cached, logicalTtl, timeUnit);
            }
            if (hot) {
                localCache.put(key, cached);
            }
            if (expireTime == null || expireTime.isAfter(LocalDateTime.now())) {
                return cached;
            }
            rebuildAsync(lockPrefix + id, key, id, dbFallback, logicalTtl, timeUnit, hot);
            return cached;
        }
        if (json != null) {
            return null;
        }

        return loadMissingValue(
                lockPrefix + id,
                key,
                id,
                type,
                dbFallback,
                logicalTtl,
                timeUnit,
                hot
        );
    }

    /**
     * Redis-only benchmark path. It understands the same logical-expiration
     * payload as the production path but intentionally bypasses Caffeine and
     * hot-key accounting so the two cache tiers can be compared independently.
     */
    public <R, ID> R queryRedisOnly(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long logicalTtl,
            TimeUnit timeUnit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return readCachedValue(json, type);
        }
        if (json != null) {
            return null;
        }
        R value = dbFallback.apply(id);
        if (value == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        setWithLogicalExpire(key, value, logicalTtl, timeUnit);
        return value;
    }

    private <R, ID> R loadMissingValue(
            String lockKey,
            String cacheKey,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long logicalTtl,
            TimeUnit timeUnit,
            boolean hot) {
        RLock lock = redissonClient.getLock(lockKey);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(200, 10, TimeUnit.SECONDS);
            if (!acquired) {
                String retryJson = stringRedisTemplate.opsForValue().get(cacheKey);
                return StrUtil.isBlank(retryJson) ? null : readCachedValue(retryJson, type);
            }

            String doubleCheck = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StrUtil.isNotBlank(doubleCheck)) {
                redisHits.increment();
                R cached = readCachedValue(doubleCheck, type);
                if (hot) {
                    localCache.put(cacheKey, cached);
                }
                return cached;
            }
            if (doubleCheck != null) {
                return null;
            }

            databaseLoads.increment();
            R value = dbFallback.apply(id);
            if (value == null) {
                stringRedisTemplate.opsForValue().set(
                        cacheKey,
                        "",
                        CACHE_NULL_TTL,
                        TimeUnit.MINUTES
                );
                return null;
            }
            setWithLogicalExpire(cacheKey, value, logicalTtl, timeUnit);
            if (hot) {
                localCache.put(cacheKey, value);
            }
            return value;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private <R, ID> void rebuildAsync(
            String lockKey,
            String cacheKey,
            ID id,
            Function<ID, R> dbFallback,
            Long logicalTtl,
            TimeUnit timeUnit,
            boolean hot) {
        RLock lock = redissonClient.getLock(lockKey);
        if (!lock.tryLock()) {
            return;
        }
        rebuildExecutor.submit(() -> {
            try {
                databaseLoads.increment();
                R value = dbFallback.apply(id);
                if (value == null) {
                    stringRedisTemplate.opsForValue().set(
                            cacheKey,
                            "",
                            CACHE_NULL_TTL,
                            TimeUnit.MINUTES
                    );
                    localCache.invalidate(cacheKey);
                } else {
                    setWithLogicalExpire(cacheKey, value, logicalTtl, timeUnit);
                    if (hot) {
                        localCache.put(cacheKey, value);
                    }
                }
                rebuilds.increment();
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        });
    }

    private boolean recordAndCheckHot(String businessKey) {
        try {
            long now = System.currentTimeMillis();
            String requestId = now + "-" + UUID.randomUUID();
            Long count = stringRedisTemplate.execute(
                    HOT_KEY_SCRIPT,
                    Collections.singletonList(HOT_KEY_PREFIX + businessKey),
                    String.valueOf(now),
                    String.valueOf(HOT_KEY_WINDOW_MILLIS),
                    requestId,
                    String.valueOf(Math.max(1, HOT_KEY_WINDOW_MILLIS / 1_000 + 5))
            );
            return count != null && count >= HOT_KEY_THRESHOLD;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private <R> R readCachedValue(String json, Class<R> type) {
        JSONObject object = JSONUtil.parseObj(json);
        if (!object.containsKey("data") || !object.containsKey("expireTime")) {
            return JSONUtil.toBean(json, type);
        }
        Object data = object.get("data");
        return JSONUtil.toBean(JSONUtil.parseObj(data), type);
    }

    private LocalDateTime readExpireTime(String json) {
        JSONObject object = JSONUtil.parseObj(json);
        if (!object.containsKey("expireTime")) {
            return null;
        }
        return object.get("expireTime", LocalDateTime.class);
    }

    public void invalidate(String key) {
        localCache.invalidate(key);
        stringRedisTemplate.delete(key);
    }

    public void invalidateLocal(String key) {
        localCache.invalidate(key);
    }

    public Map<String, Object> metrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        long requestCount = requests.sum();
        long localHitCount = localHits.sum();
        long redisHitCount = redisHits.sum();
        metrics.put("requests", requestCount);
        metrics.put("localHits", localHitCount);
        metrics.put("redisHits", redisHitCount);
        metrics.put("databaseLoads", databaseLoads.sum());
        metrics.put("rebuilds", rebuilds.sum());
        metrics.put(
                "overallHitRate",
                requestCount == 0 ? 0D : (double) (localHitCount + redisHitCount) / requestCount
        );
        metrics.put("caffeine", localCache.stats().toString());
        metrics.put("localSize", localCache.estimatedSize());
        return metrics;
    }

    public void resetMetrics() {
        requests.reset();
        localHits.reset();
        redisHits.reset();
        databaseLoads.reset();
        rebuilds.reset();
    }
}
