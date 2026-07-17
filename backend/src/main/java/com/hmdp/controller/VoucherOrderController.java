package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    @PostMapping("benchmark/sync/{id}")
    public Result seckillVoucherSynchronously(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucherSynchronously(voucherId);
    }

    @PostMapping("{id}/pay")
    public Result payOrder(@PathVariable("id") Long orderId) {
        return voucherOrderService.payOrder(orderId);
    }

    @GetMapping("metrics")
    public Result processingMetrics() {
        return Result.ok(voucherOrderService.processingMetrics());
    }

    @PostMapping("metrics/reset")
    public Result resetProcessingMetrics() {
        voucherOrderService.resetProcessingMetrics();
        return Result.ok();
    }
}
