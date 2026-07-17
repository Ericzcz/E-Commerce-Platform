package com.hmdp.service;

import com.hmdp.utils.CacheClient;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_INVALIDATE_STREAM;

/**
 * Broadcasts local-cache invalidations through one Redis Stream consumer group
 * per application instance. A local retry queue covers temporary Redis errors;
 * cache TTL remains the final fallback.
 */
@Service
public class CacheInvalidationService {

    private final StringRedisTemplate redisTemplate;
    private final CacheClient cacheClient;
    private final ConcurrentLinkedQueue<String> retryQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "cache-invalidation-consumer");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String groupName = "cache-node-" + UUID.randomUUID().toString().replace("-", "");
    private final String consumerName = instanceName();

    public CacheInvalidationService(StringRedisTemplate redisTemplate, CacheClient cacheClient) {
        this.redisTemplate = redisTemplate;
        this.cacheClient = cacheClient;
    }

    @PostConstruct
    public void start() {
        createGroup();
        consumerExecutor.submit(this::consume);
    }

    public void invalidate(String key) {
        cacheClient.invalidateLocal(key);
        try {
            redisTemplate.delete(key);
            publish(key);
        } catch (RuntimeException failure) {
            retryQueue.offer(key);
        }
    }

    @Scheduled(fixedDelay = 1_000L)
    public void retryFailedInvalidations() {
        int remaining = Math.min(retryQueue.size(), 100);
        for (int i = 0; i < remaining; i++) {
            String key = retryQueue.poll();
            if (key == null) {
                return;
            }
            try {
                redisTemplate.delete(key);
                publish(key);
            } catch (RuntimeException failure) {
                retryQueue.offer(key);
                return;
            }
        }
    }

    private void publish(String key) {
        MapRecord<String, String, String> record = MapRecord.create(
                CACHE_SHOP_INVALIDATE_STREAM,
                Collections.singletonMap("key", key)
        );
        redisTemplate.opsForStream().add(record);
        redisTemplate.opsForStream().trim(CACHE_SHOP_INVALIDATE_STREAM, 10_000);
    }

    private void consume() {
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                        Consumer.from(groupName, consumerName),
                        StreamReadOptions.empty().count(20).block(Duration.ofSeconds(2)),
                        StreamOffset.create(CACHE_SHOP_INVALIDATE_STREAM, ReadOffset.lastConsumed())
                );
                if (records == null) {
                    continue;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    Object key = record.getValue().get("key");
                    if (key != null) {
                        cacheClient.invalidateLocal(key.toString());
                    }
                    redisTemplate.opsForStream().acknowledge(
                            CACHE_SHOP_INVALIDATE_STREAM,
                            groupName,
                            record.getId()
                    );
                }
            } catch (RuntimeException failure) {
                sleepQuietly(500);
                createGroup();
            }
        }
    }

    private void createGroup() {
        try {
            redisTemplate.execute((RedisCallback<Object>) connection -> executeCreateGroup(connection));
        } catch (RuntimeException ignored) {
            // BUSYGROUP means the instance group already exists.
        }
    }

    private Object executeCreateGroup(RedisConnection connection) {
        return connection.execute(
                "XGROUP",
                bytes("CREATE"),
                bytes(CACHE_SHOP_INVALIDATE_STREAM),
                bytes(groupName),
                // This group represents a newly started cache node and should
                // receive future broadcasts, not replay historical invalidations.
                bytes("$"),
                bytes("MKSTREAM")
        );
    }

    private byte[] bytes(String value) {
        return redisTemplate.getStringSerializer().serialize(value);
    }

    private static String instanceName() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        } catch (Exception ignored) {
            return "instance-" + UUID.randomUUID();
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        consumerExecutor.shutdownNow();
    }
}
