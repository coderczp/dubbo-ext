package com.czp.dubbo.ext;

import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function: 基于守护进程timer
 * 
 * @author: coder_czp@126.com
 * @date: 2016年1月25日
 * 
 */
public class DaemonTimer implements Runnable {
    
    private static class TimerTask {
        
        Runnable task;
        
        int peroid;
        
        int times;
        
        TimerTask(Runnable task, int peroid) {
            this.task = task;
            this.peroid = peroid;
        }
        
    }
    
    /** 需要调度的timer task */
    private CopyOnWriteArraySet<TimerTask> tasks = new CopyOnWriteArraySet<TimerTask>();
    
    private static Logger log = LoggerFactory.getLogger(DaemonTimer.class);
    
    private static final DaemonTimer TIMER = new DaemonTimer();
    
    private Thread timerThread;
    
    private DaemonTimer() {
        timerThread = new Thread(this, "DaemonTimer");
        timerThread.setDaemon(true);
    }
    
    public static DaemonTimer getInstance() {
        return TIMER;
    }
    
    public void addTask(Runnable runnable, int peroid) {
        tasks.add(new TimerTask(runnable, peroid));
        if (!timerThread.isAlive())
            timerThread.start();
    }
    
    /**
     * 删除timer task
     * 
     * @param runnable
     */
    public void removeTask(Runnable runnable) {
        for (TimerTask item : tasks) {
            if (item.task.equals(runnable)) {
                tasks.remove(runnable);
                return;
            }
        }
    }
    
    public void run() {
        int sleepTime = Integer.valueOf(System.getProperty("daemon.timer.peroid", "300"));
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(sleepTime);
                for (TimerTask item : tasks) {
                    if (item.times < item.peroid) {
                        item.times += sleepTime;
                        continue;
                    }
                    item.task.run();
                    item.times = 0;
                }
            } catch (Exception e) {
                log.error("DeamonTimer error", e);
            }
        }
    }
}
