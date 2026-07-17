package com.hmdp;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class OrderStateConcurrencyTest {

    private static final long CAS_VOUCHER_ID = 99003L;
    private static final long EXPIRED_VOUCHER_ID = 99004L;
    private static final int ROUNDS = 100;

    @Resource
    private IVoucherOrderService orderService;
    @Resource
    private VoucherOrderServiceImpl orderServiceImpl;
    @Resource
    private JdbcTemplate jdbcTemplate;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void paymentAndClosingMustHaveSingleWinner() throws Exception {
        createVoucher(CAS_VOUCHER_ID, 0);
        for (int i = 0; i < ROUNDS; i++) {
            jdbcTemplate.update(
                    "INSERT INTO tb_voucher_order " +
                            "(id,user_id,voucher_id,status,create_time) " +
                            "VALUES (?,?,?,?,UTC_TIMESTAMP())",
                    990030000L + i,
                    i + 1L,
                    CAS_VOUCHER_ID,
                    1
            );
        }

        ExecutorService executor = Executors.newFixedThreadPool(32);
        long started = System.nanoTime();
        int conflicts = 0;
        try {
            for (int i = 0; i < ROUNDS; i++) {
                long orderId = 990030000L + i;
                long userId = i + 1L;
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> payment = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    UserDTO user = new UserDTO();
                    user.setId(userId);
                    UserHolder.saveUser(user);
                    try {
                        Result result = orderService.payOrder(orderId);
                        return Boolean.TRUE.equals(result.getSuccess());
                    } finally {
                        UserHolder.removeUser();
                    }
                });
                Future<Boolean> closing = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return orderService.closeUnpaidOrder(orderId, CAS_VOUCHER_ID);
                });
                ready.await(5, TimeUnit.SECONDS);
                start.countDown();
                boolean paid = payment.get(5, TimeUnit.SECONDS);
                boolean closed = closing.get(5, TimeUnit.SECONDS);
                if (paid == closed) {
                    conflicts++;
                }
            }
        } finally {
            executor.shutdownNow();
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        Integer paid = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=? AND status=2",
                Integer.class,
                CAS_VOUCHER_ID
        );
        Integer cancelled = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=? AND status=4",
                Integer.class,
                CAS_VOUCHER_ID
        );
        Integer invalid = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order " +
                        "WHERE voucher_id=? AND status NOT IN (2,4)",
                Integer.class,
                CAS_VOUCHER_ID
        );
        Integer restoredStock = jdbcTemplate.queryForObject(
                "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?",
                Integer.class,
                CAS_VOUCHER_ID
        );

        Assertions.assertEquals(0, conflicts);
        Assertions.assertEquals(ROUNDS, paid + cancelled);
        Assertions.assertEquals(0, invalid);
        Assertions.assertEquals(cancelled, restoredStock);

        writeReport(
                "order-cas.json",
                "{\n" +
                        "  \"rounds\": " + ROUNDS + ",\n" +
                        "  \"paid\": " + paid + ",\n" +
                        "  \"cancelled\": " + cancelled + ",\n" +
                        "  \"invalidStates\": " + invalid + ",\n" +
                        "  \"doubleWinnersOrLosers\": " + conflicts + ",\n" +
                        "  \"restoredDatabaseStock\": " + restoredStock + ",\n" +
                        "  \"durationMillis\": " + elapsedMillis + "\n" +
                        "}\n"
        );
    }

    @Test
    void expiredOrderMustCloseAndRestoreStockOnce() throws Exception {
        createVoucher(EXPIRED_VOUCHER_ID, 0);
        stringRedisTemplate.opsForValue().set("seckill:stock:" + EXPIRED_VOUCHER_ID, "0");
        long orderId = 990040001L;
        jdbcTemplate.update(
                "INSERT INTO tb_voucher_order " +
                        "(id,user_id,voucher_id,status,create_time) " +
                        "VALUES (?,?,?,1,UTC_TIMESTAMP()-INTERVAL 20 MINUTE)",
                orderId,
                1L,
                EXPIRED_VOUCHER_ID
        );

        long started = System.nanoTime();
        orderServiceImpl.closeExpiredOrders();
        orderServiceImpl.closeExpiredOrders();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        Integer status = jdbcTemplate.queryForObject(
                "SELECT status FROM tb_voucher_order WHERE id=?",
                Integer.class,
                orderId
        );
        Integer mysqlStock = jdbcTemplate.queryForObject(
                "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?",
                Integer.class,
                EXPIRED_VOUCHER_ID
        );
        String redisStock = stringRedisTemplate.opsForValue().get(
                "seckill:stock:" + EXPIRED_VOUCHER_ID
        );
        Assertions.assertEquals(4, status);
        Assertions.assertEquals(1, mysqlStock);
        Assertions.assertEquals("1", redisStock);

        writeReport(
                "auto-close.json",
                "{\n" +
                        "  \"expiredOrders\": 1,\n" +
                        "  \"taskExecutions\": 2,\n" +
                        "  \"finalStatus\": 4,\n" +
                        "  \"mysqlStockRestored\": 1,\n" +
                        "  \"redisStockRestored\": 1,\n" +
                        "  \"duplicateCompensations\": 0,\n" +
                        "  \"durationMillis\": " + elapsedMillis + "\n" +
                        "}\n"
        );
    }

    private void createVoucher(long voucherId, int stock) {
        jdbcTemplate.update(
                "INSERT INTO tb_voucher " +
                        "(id,shop_id,title,pay_value,actual_value,type,status) " +
                        "VALUES (?,1,?,100,1000,1,1)",
                voucherId,
                "Integration Test Voucher " + voucherId
        );
        jdbcTemplate.update(
                "INSERT INTO tb_seckill_voucher " +
                        "(voucher_id,stock,begin_time,end_time) " +
                        "VALUES (?, ?, UTC_TIMESTAMP()-INTERVAL 1 HOUR, " +
                        "UTC_TIMESTAMP()+INTERVAL 1 HOUR)",
                voucherId,
                stock
        );
    }

    private void writeReport(String filename, String content) throws Exception {
        Path output = Paths.get("benchmarks", "results", filename);
        Files.createDirectories(output.getParent());
        Files.write(output, content.getBytes(StandardCharsets.UTF_8));
    }

    @AfterEach
    void cleanup() {
        List<Long> vouchers = new ArrayList<>();
        vouchers.add(CAS_VOUCHER_ID);
        vouchers.add(EXPIRED_VOUCHER_ID);
        for (Long voucherId : vouchers) {
            jdbcTemplate.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", voucherId);
            jdbcTemplate.update("DELETE FROM tb_seckill_voucher WHERE voucher_id=?", voucherId);
            jdbcTemplate.update("DELETE FROM tb_voucher WHERE id=?", voucherId);
            stringRedisTemplate.delete("seckill:stock:" + voucherId);
            stringRedisTemplate.delete("seckill:order:" + voucherId);
            stringRedisTemplate.delete("seckill:meta:" + voucherId);
        }
    }
}
