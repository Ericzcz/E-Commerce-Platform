package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.net.InetAddress;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Service
public class VoucherOrderServiceImpl
        extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final int UNPAID = 1;
    private static final int PAID = 2;
    private static final int CANCELLED = 4;
    private static final int MAX_DELIVERIES = 5;
    private static final long PAYMENT_TIMEOUT_MINUTES = 15L;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> COMPENSATE_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
        COMPENSATE_SCRIPT = new DefaultRedisScript<>();
        COMPENSATE_SCRIPT.setLocation(new ClassPathResource("seckill-compensate.lua"));
        COMPENSATE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Lazy
    @Resource
    private IVoucherOrderService self;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LongAdder processedOrders = new LongAdder();
    private final AtomicLong firstProcessedNanos = new AtomicLong();
    private final AtomicLong lastProcessedNanos = new AtomicLong();
    private final ConcurrentLinkedQueue<Long> stockCompensationRetries = new ConcurrentLinkedQueue<>();
    private final String consumerName = createConsumerName();
    private final ExecutorService orderExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "voucher-order-consumer");
        thread.setDaemon(true);
        return thread;
    });

    @PostConstruct
    public void init() {
        createConsumerGroup();
        initializeMissingSeckillMetadata();
        orderExecutor.submit(this::consumeOrders);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        ensureVoucherMetadata(voucherId);
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId),
                String.valueOf(System.currentTimeMillis())
        );
        int code = result == null ? -1 : result.intValue();
        if (code == 0) {
            return Result.ok(orderId);
        }
        if (code == 1) {
            return Result.fail("Insufficient stock.");
        }
        if (code == 2) {
            return Result.fail("Duplicate order.");
        }
        if (code == 3) {
            return Result.fail("Sale has not started.");
        }
        if (code == 4) {
            return Result.fail("Sale has ended.");
        }
        return Result.fail("Ordering service is temporarily unavailable.");
    }

    /**
     * Database-bound baseline retained only for reproducible benchmark
     * comparison with the Redis Stream asynchronous path.
     */
    @Override
    @Transactional
    public Result seckillVoucherSynchronously(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        if (voucher == null || now.isBefore(voucher.getBeginTime())) {
            return Result.fail("Sale has not started.");
        }
        if (now.isAfter(voucher.getEndTime())) {
            return Result.fail("Sale has ended.");
        }
        int existing = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (existing > 0) {
            return Result.fail("Duplicate order.");
        }
        boolean deducted = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!deducted) {
            return Result.fail("Insufficient stock.");
        }
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder order = new VoucherOrder()
                .setId(orderId)
                .setUserId(userId)
                .setVoucherId(voucherId)
                .setStatus(UNPAID);
        save(order);
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder order) {
        if (getById(order.getId()) != null) {
            return;
        }
        boolean deducted = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", order.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!deducted) {
            throw new IllegalStateException("Database stock is insufficient.");
        }
        order.setStatus(UNPAID);
        save(order);
    }

    @Override
    public Result payOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        boolean paid = update()
                .set("status", PAID)
                .set("pay_time", LocalDateTime.now(ZoneOffset.UTC))
                .eq("id", orderId)
                .eq("user_id", userId)
                .eq("status", UNPAID)
                .update();
        return paid
                ? Result.ok()
                : Result.fail("The order is not payable or has already been processed.");
    }

    @Override
    @Transactional
    public boolean closeUnpaidOrder(Long orderId, Long voucherId) {
        boolean closed = update()
                .set("status", CANCELLED)
                .eq("id", orderId)
                .eq("status", UNPAID)
                .update();
        if (!closed) {
            return false;
        }
        boolean restored = seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", voucherId)
                .update();
        if (!restored) {
            throw new IllegalStateException("Failed to restore database stock.");
        }
        return true;
    }

    @Scheduled(fixedDelay = 60_000L)
    public void closeExpiredOrders() {
        LocalDateTime deadline = LocalDateTime.now(ZoneOffset.UTC)
                .minusMinutes(PAYMENT_TIMEOUT_MINUTES);
        List<VoucherOrder> expired = query()
                .eq("status", UNPAID)
                .lt("create_time", deadline)
                .orderByAsc("create_time")
                .last("LIMIT 100")
                .list();
        for (VoucherOrder order : expired) {
            if (self.closeUnpaidOrder(order.getId(), order.getVoucherId())) {
                restoreRedisStock(order.getVoucherId());
            }
        }
    }

    @Scheduled(fixedDelay = 1_000L)
    public void retryRedisStockCompensation() {
        int size = Math.min(stockCompensationRetries.size(), 100);
        for (int i = 0; i < size; i++) {
            Long voucherId = stockCompensationRetries.poll();
            if (voucherId == null) {
                return;
            }
            restoreRedisStock(voucherId);
        }
    }

    private void consumeOrders() {
        recoverPendingOrders();
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(STREAM_ORDERS_GROUP, consumerName),
                        StreamReadOptions.empty().count(10).block(Duration.ofSeconds(2)),
                        StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.lastConsumed())
                );
                if (records == null || records.isEmpty()) {
                    recoverPendingOrders();
                    continue;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    processRecord(record);
                }
            } catch (RuntimeException failure) {
                if (!running.get() || Thread.currentThread().isInterrupted()) {
                    return;
                }
                log.error("Failed to consume voucher orders.", failure);
                sleepQuietly(500);
                createConsumerGroup();
                recoverPendingOrders();
            }
        }
    }

    private void recoverPendingOrders() {
        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(STREAM_ORDERS_GROUP, consumerName),
                        StreamReadOptions.empty().count(10),
                        StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.from("0"))
                );
                if (records == null || records.isEmpty()) {
                    return;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    processRecord(record);
                }
            } catch (RuntimeException failure) {
                log.error("Failed to recover pending voucher orders.", failure);
                return;
            }
        }
    }

    private void processRecord(MapRecord<String, Object, Object> record) {
        VoucherOrder order = BeanUtil.fillBeanWithMap(record.getValue(), new VoucherOrder(), true);
        try {
            self.createVoucherOrder(order);
            recordProcessedOrder();
            acknowledge(record);
            clearRetryCount(record);
        } catch (DuplicateKeyException duplicate) {
            if (getById(order.getId()) != null) {
                acknowledge(record);
                clearRetryCount(record);
            } else {
                moveToDeadLetterAfterLimit(record, order, duplicate);
            }
        } catch (RuntimeException failure) {
            moveToDeadLetterAfterLimit(record, order, failure);
        }
    }

    private void moveToDeadLetterAfterLimit(
            MapRecord<String, Object, Object> record,
            VoucherOrder order,
            RuntimeException failure) {
        String retryKey = STREAM_ORDERS_KEY + ":retries";
        Long attempts = stringRedisTemplate.opsForHash().increment(
                retryKey,
                record.getId().getValue(),
                1
        );
        log.error(
                "Order {} failed on delivery {}.",
                order.getId(),
                attempts,
                failure
        );
        if (attempts == null || attempts < MAX_DELIVERIES) {
            return;
        }
        Map<String, String> deadMessage = new HashMap<>();
        record.getValue().forEach((key, value) -> deadMessage.put(key.toString(), value.toString()));
        deadMessage.put("sourceRecordId", record.getId().getValue());
        deadMessage.put("error", failure.getClass().getSimpleName());
        stringRedisTemplate.opsForStream().add(
                MapRecord.create(STREAM_ORDERS_DEAD_KEY, deadMessage)
        );
        compensateReservation(order.getVoucherId(), order.getUserId());
        acknowledge(record);
        clearRetryCount(record);
    }

    private void acknowledge(MapRecord<String, Object, Object> record) {
        stringRedisTemplate.opsForStream().acknowledge(
                STREAM_ORDERS_KEY,
                STREAM_ORDERS_GROUP,
                record.getId()
        );
    }

    private void clearRetryCount(MapRecord<String, Object, Object> record) {
        stringRedisTemplate.opsForHash().delete(
                STREAM_ORDERS_KEY + ":retries",
                record.getId().getValue()
        );
    }

    private void compensateReservation(Long voucherId, Long userId) {
        stringRedisTemplate.execute(
                COMPENSATE_SCRIPT,
                java.util.Arrays.asList(
                        SECKILL_STOCK_KEY + voucherId,
                        SECKILL_ORDER_KEY + voucherId
                ),
                userId.toString()
        );
    }

    private void restoreRedisStock(Long voucherId) {
        try {
            stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
        } catch (RuntimeException failure) {
            stockCompensationRetries.offer(voucherId);
        }
    }

    private void initializeMissingSeckillMetadata() {
        List<SeckillVoucher> vouchers = seckillVoucherService.list();
        for (SeckillVoucher voucher : vouchers) {
            writeMetadata(voucher);
        }
    }

    private void ensureVoucherMetadata(Long voucherId) {
        String metaKey = "seckill:meta:" + voucherId;
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(metaKey))) {
            return;
        }
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher != null) {
            writeMetadata(voucher);
            stringRedisTemplate.opsForValue().setIfAbsent(
                    SECKILL_STOCK_KEY + voucherId,
                    voucher.getStock().toString()
            );
        }
    }

    private void writeMetadata(SeckillVoucher voucher) {
        String metaKey = "seckill:meta:" + voucher.getVoucherId();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("begin", String.valueOf(toEpochMilli(voucher.getBeginTime())));
        metadata.put("end", String.valueOf(toEpochMilli(voucher.getEndTime())));
        stringRedisTemplate.opsForHash().putAll(metaKey, metadata);
    }

    private long toEpochMilli(LocalDateTime value) {
        return value.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private void createConsumerGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection ->
                    executeCreateGroup(connection)
            );
        } catch (RuntimeException ignored) {
            // BUSYGROUP means the consumer group already exists.
        }
    }

    private Object executeCreateGroup(RedisConnection connection) {
        return connection.execute(
                "XGROUP",
                bytes("CREATE"),
                bytes(STREAM_ORDERS_KEY),
                bytes(STREAM_ORDERS_GROUP),
                bytes("0"),
                bytes("MKSTREAM")
        );
    }

    private byte[] bytes(String value) {
        return stringRedisTemplate.getStringSerializer().serialize(value);
    }

    private static String createConsumerName() {
        try {
            // Stable on restart so this instance can recover its own Pending entries.
            return "consumer-" + InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            return "consumer-default";
        }
    }

    private void recordProcessedOrder() {
        long now = System.nanoTime();
        firstProcessedNanos.compareAndSet(0L, now);
        lastProcessedNanos.set(now);
        processedOrders.increment();
    }

    @Override
    public Map<String, Object> processingMetrics() {
        long count = processedOrders.sum();
        long first = firstProcessedNanos.get();
        long last = lastProcessedNanos.get();
        double durationSeconds = count <= 1 ? 0D : (last - first) / 1_000_000_000D;
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("processedOrders", count);
        metrics.put("durationSeconds", durationSeconds);
        metrics.put(
                "ordersPerSecond",
                durationSeconds == 0D ? count : count / durationSeconds
        );
        return metrics;
    }

    @Override
    public void resetProcessingMetrics() {
        processedOrders.reset();
        firstProcessedNanos.set(0L);
        lastProcessedNanos.set(0L);
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        orderExecutor.shutdownNow();
    }
}
