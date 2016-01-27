package com.czp.dubbo.ext;

import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.common.utils.ConfigUtils;

/**
 * Function: 对相同的参数结果进行缓存
 * 
 * @author: coder_czp@126.com
 * @date: 2016年1月26日
 * 
 */
public class CacheFilter implements Filter, Runnable {
    
    private static Logger logger = LoggerFactory.getLogger("run");
    
    /** 要缓存的方法前缀,默认 */
    private String[] methodPrefix;
    
    /** 缓存查询结果,定时清空 */
    private HashMap<String, Object> cache = new HashMap<String, Object>(500, 0.75f);
    
    public CacheFilter() {
        int peroid = Integer.valueOf(ConfigUtils.getProperty("dubbo.cache.filter.clear.peroid", "5000"));
        String prefix = ConfigUtils.getProperty("dubbo.cache.filter.method.name.prefix", "query,get,select");
        methodPrefix = prefix.split(",");
        DaemonTimer.getInstance().addTask(this, peroid);
        logger.info("CacheFilter is running,method prefix:{} peroid:{}", Arrays.toString(methodPrefix), peroid);
    }
    
    public Result invoke(Invoker<?> invoker, Invocation invo)
        throws RpcException {
        Result result;
        long start = System.currentTimeMillis();
        String method = invo.getMethodName();
        StringBuilder key = new StringBuilder(120);
        key.append(invo.getInvoker().getInterface().getName()).append(".").append(method);
        
        for (String name : methodPrefix) {
            if (method.startsWith(name)) {
                key.append("[");
                Object[] args = invo.getArguments();
                for (int i = 0; i < args.length; i++) {
                    key.append(args[i]).append(",");
                }
                key.append("]");
                String cacheKey = key.toString();
                Object value = cache.get(cacheKey);
                if (value != null) {
                    logger.info("cache hit:{},time:{}", cacheKey, (System.currentTimeMillis() - start));
                    return new RpcResult(value);
                }
                result = invoker.invoke(invo);
                if (!result.hasException()) {
                    cache.put(cacheKey, result.getValue());
                }
                logger.info("call server:{} time:{}", cacheKey, (System.currentTimeMillis() - start));
                return result;
            }
        }
        result = invoker.invoke(invo);
        logger.info("call server:{} time:{}", key, (System.currentTimeMillis() - start));
        return result;
    }
    
    public void run() {
        cache.clear();
        logger.debug("clear all cache");
    }
    
}
