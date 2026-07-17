package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;
import java.util.Map;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result seckillVoucherSynchronously(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    Result payOrder(Long orderId);

    boolean closeUnpaidOrder(Long orderId, Long voucherId);

    Map<String, Object> processingMetrics();

    void resetProcessingMetrics();
}
