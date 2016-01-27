package com.czp.dubbo.ext;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;

/**
 * Function:扩展dubbo增加流控功能,因为要统计耗时,选择扩展Filter
 * 
 * <pre>
 *   使用:<dubbo:consumer filter="flowControlFilter" >
 *   满足以下任一场景将窗口调小:<br>
 *         1.失败率大于设定的值 FAILD_PERCENT_THRESHOLD
 *                  ServerStatus.failCount/requestCount>FAILD_PERCENT_THRESHOLD
 *         2.一段时间内慢响应次数大于SLOW_RESPONSE_THRESHOLD 
 *                ServerStatus.slowReqCount>SLOW_RESPONSE_THRESHOLD
 *                
 *   定制化参数在dubbo.properties配置:
 *     #请求进入tomcat到调用dubbo之间的时差大于该值时会终止请求,单位:毫秒,默认10s
 *     dubbo.flowcontrol.record.web.abort.threshold=10000
 *     #当慢响应大于该值时将窗口调小。默认3%
 *     dubbo.flowcontrol.slow.ratio.threshold
 *     #当失败率大于该值时将窗口调小。默认15%
 *     dubbo.flowcontrol.faild.ratio.threshold=0.15
 *     #每次请求响应耗时大于该值时会统计,单位:毫秒,默认5s
 *     dubbo.flowcontrol.record.response.threshold=5000
 *     #清除计数的周期,单位:毫秒,默认5s清除
 *     dubbo.flowcontrol.clear.peroid=5000
 *     #异步检测RPC请求耗时的Timer调度周期,默认1s
 *     dubbo.flowcontrol.timer=1000
 *     #滑动窗口滑动比例,默认0.3
 *     dubbo.flowcontrol.slide.ratio=0.3
 *     #滑动窗口的最大值,默认10000
 *     dubbo.flowcontrol.max.size=10000
 *     #滑动窗口的最小值,默认1000
 *     dubbo.flowcontrol.min.size=1
 * 
 * </pre>
 * 
 * @author: jeff.cao@aoliday.com
 * @date: 2016年1月8日
 * 
 */
public class FlowControlFilter implements Filter, Runnable {
    
    /** 放在同一个文件,方便理解 */
    private class ServerStatus {
        
        /** 失败或 慢响应开始统计的时间,用于定时清除计数 */
        AtomicLong recordStart = new AtomicLong();
        
        /** 总失败次数 */
        AtomicInteger failCount = new AtomicInteger();
        
        /** 总请求次数 */
        AtomicInteger requestCount = new AtomicInteger();
        
        /** 慢响应次数 */
        AtomicInteger slowRespCount = new AtomicInteger();
        
        /** 周期内最大请求数,会随后端服务状态调整 */
        AtomicInteger maxRequest = new AtomicInteger(initRequest);
        
        public void resetCounter(long now) {
            String cur = this.toString();
            failCount.set(0);
            requestCount.set(1);
            recordStart.set(now);
            slowRespCount.set(0);
            logger.debug("reset counter:{} {}", cur, this);
        }
        
        @Override
        public String toString() {
            return "ServerStatus [recordStart=" + recordStart + ", failCount=" + failCount + ", requestCount="
                + requestCount + ", slowRespCount=" + slowRespCount + ", maxRequest=" + maxRequest + "]";
        }
        
    }
    
    /** 滑动窗口的最大值 */
    private int maxRequest;
    
    /** 滑动窗口的最小值 */
    private int minRequest;
    
    /** 窗口初始化大小 */
    private int initRequest;
    
    /** 滑动比例,每次增长或降低按此比例进行 */
    private float slideRatio;
    
    /** 异步检测RPC请求耗时的Timer调度周期 */
    private int timerPeroid;
    
    /** 计数统计周期,单位:毫秒 */
    private int clearPeroid;
    
    /** 慢响比例数超过该值时下调滑动窗口 */
    private float slowRespThreshold;
    
    /** 失败比例阈值,失败率大于此值时下调滑动窗口 */
    private float failRatioThreshold;
    
    /** 每次响应耗时大于该值时累加慢响应,单位:毫秒 */
    private int recordRespThreshold;
    
    private static Logger logger = LoggerFactory.getLogger("run");
    
    /** RPC task的临时缓存队列 */
    private BlockingQueue<Invocation> rpcs = new LinkedBlockingQueue<Invocation>();
    
    /** 服务状态统计，这是线程安全的 */
    private ConcurrentHashMap<String, ServerStatus> statusMap = new ConcurrentHashMap<String, ServerStatus>();
    
    public FlowControlFilter() {
        
        recordRespThreshold = (int)getNumberConfig("dubbo.flowcontrol.record.respon.threshold", "5000");
        slowRespThreshold = getNumberConfig("dubbo.flowcontrol.slow.ratio.threshold", "0.03");
        failRatioThreshold = getNumberConfig("dubbo.flowcontrol.faild.ratio.threshold", "0.25");
        clearPeroid = (int)getNumberConfig("dubbo.flowcontrol.clear.peroid", "5000");
        maxRequest = (int)getNumberConfig("dubbo.flowcontrol.max.size", "10000");
        initRequest = (int)getNumberConfig("dubbo.flowcontrol.init.size", "500");
        timerPeroid = (int)getNumberConfig("dubbo.flowcontrol.timer", "500");
        minRequest = (int)getNumberConfig("dubbo.flowcontrol.min.size", "500");
        slideRatio = getNumberConfig("dubbo.flowcontrol.slide.ratio", "0.3");
        
        minRequest = Math.max(minRequest, 100);
        
        Thread timer = new Thread(this);
        timer.setName("FlowControlFilter-Timer");
        timer.setDaemon(true);
        timer.start();
        logger.info("FlowControlFilter is start");
    }
    
    public Result invoke(Invoker<?> invoker, Invocation invocation)
        throws RpcException {
        Map<String, String> attach = invocation.getAttachments();
        /* 按IP:PORT统计,支持同一主机起多个服务 */
        String address = invoker.getUrl().getAddress();
        ServerStatus status = statusMap.get(address);
        long curTime = System.currentTimeMillis();
        if (status == null) {
            statusMap.putIfAbsent(address, new ServerStatus());
            status = statusMap.get(address);
            status.recordStart.compareAndSet(0, curTime);
        }
        /* 将调用放到缓存队列 */
        attach.put("start", String.valueOf(curTime));
        attach.put("addr", address);
        rpcs.add(invocation);
        
        abortIfReachMaxRequest(invocation, address, status);
        Result result = invoker.invoke(invocation);
        if (result.hasException()) {
            attach.put("hasError", String.valueOf(result.getException() instanceof RpcException));
        }
        return result;
    }
    
    /**
     * 达到当前周期的极限,中断请求
     * 
     * @param invocation
     * @param address
     * @param status
     */
    private void abortIfReachMaxRequest(Invocation invocation, String address, ServerStatus status) {
        int maxReq = status.maxRequest.get();
        int reqCount = status.requestCount.getAndIncrement();
        if (reqCount > maxReq) {
            String info = String.format("max req:[%s] reqCount:[%s]", maxReq, reqCount);
            logger.warn(">>RPC warn:abort[{}],{}", createLogInfo(address, invocation), info);
            throw new RpcException(address.concat(" is busy"));
        }
    }
    
    /**
     * 根据服务响应状态滑动最大请求窗口
     * 
     * @param status
     */
    private void modifyMaxRequest(ServerStatus status) {
        float reqCount = status.requestCount.get();
        int slowCount = status.slowRespCount.get();
        int fialCount = status.failCount.get();
        
        if (reqCount > 10f) {
            /* 响应过慢,则将窗口调小 SLIDE_PERCENT */
            float slowRatio = slowCount / reqCount;
            if (slowRatio > slowRespThreshold) {
                decreaseMaxReq(status, "slow ratio:" + slowRatio);
                return;
            }
            /* 失败率过高,则将窗口调小 SLIDE_PERCENT */
            float ratio = fialCount / reqCount;
            if (ratio > failRatioThreshold) {
                decreaseMaxReq(status, "fail ratio:" + ratio);
                return;
            }
        } else if (slowCount == 0 && fialCount == 0) {
            /* 如果没有失败也没有慢响应,则将窗口调大SLIDE_PERCENT */
            increaseMaxReq(status);
        }
        
    }
    
    /**
     * 将窗口调大
     * 
     * @param status
     */
    private void increaseMaxReq(ServerStatus status) {
        int lastVal = status.maxRequest.get();
        if (lastVal >= maxRequest)
            return;
        int newVal = (int)(lastVal + lastVal * slideRatio);
        if (newVal > maxRequest)
            newVal = maxRequest;
        status.maxRequest.set(newVal);
        logger.info("increase maxReq to:{} server is fast", newVal);
    }
    
    /**
     * 将窗口调小
     * 
     * @param status
     * @param isSlow
     * @param ratio
     */
    private void decreaseMaxReq(ServerStatus status, String info) {
        int oldVal = status.maxRequest.get();
        if (oldVal <= minRequest)
            return;
        int newVal = (int)(oldVal - oldVal * slideRatio);
        if (newVal < minRequest)
            newVal = minRequest;
        status.maxRequest.set(newVal);
        logger.info("decrease maxReq to:{},for:{}", newVal, info);
    }
    
    /**
     * 生成日志信息,记录请求的主机.类.方法
     * 
     * @param address
     * @param invocation
     * @return
     */
    private String createLogInfo(String address, Invocation invocation) {
        String method = invocation.getMethodName();
        Class<?> cls = invocation.getInvoker().getInterface();
        return address.concat(".").concat(cls.getSimpleName()).concat(".").concat(method);
    }
    
    /**
     * 根据调用信息统计后端服务状态,定时清除计数器
     */
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(timerPeroid);
                Invocation rpc = rpcs.poll();
                long now = System.currentTimeMillis();
                resetIfExpired(now);
                if (rpc == null)
                    continue;
                String endStr = rpc.getAttachment("end");
                boolean hasReturn = (endStr != null);
                long end = hasReturn ? now : Long.valueOf(endStr);
                long start = Long.valueOf(rpc.getAttachment("start"));
                ServerStatus status = statusMap.get(rpc.getAttachment("addr"));
                if ((end - start) > recordRespThreshold) {
                    status.slowRespCount.getAndIncrement();
                } else if (hasReturn) {
                    /* 没有返回,也没有超时,则重新放到队列 */
                    rpcs.add(rpc);
                }
                if (rpc.getAttachments().containsKey("hasError")) {
                    status.failCount.getAndIncrement();
                }
                modifyMaxRequest(status);
            } catch (Exception e) {
                logger.error("FlowControl-Timer error", e);
            }
        }
    }
    
    /**
     * 如果统计数据已过期,则重置计数器
     * 
     * @param times
     * @param status
     */
    private void resetIfExpired(long now) {
        for (ServerStatus status : statusMap.values()) {
            long start = status.recordStart.get();
            if (start + clearPeroid < now)
                status.resetCounter(now);
        }
    }
    
    /**
     * 获取数值配置项目
     * 
     * @param key
     * @param defaultVal
     * @return
     */
    private float getNumberConfig(String key, String defaultVal) {
        return Float.valueOf(ConfigUtils.getProperty(key, defaultVal));
    }
}
