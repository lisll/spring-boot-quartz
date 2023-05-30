package com.datapipeline.springbootquartz.bootquartz;

import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.TimeUnit;

public class ThreadRejectedExecutionHandler {

  public static void main(String[] args) {
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
//    pool.setRejectedExecutionHandler(new AbortPolicy());
     pool.setRejectedExecutionHandler(new CallerRunsPolicy());
//    pool.setRejectedExecutionHandler(new DiscardPolicy());
//    pool.setRejectedExecutionHandler(new DiscardOldestPolicy());
    for (int i = 0; i < 10; i++) {
      Runnable myTask = new Task("task-" + i);
      pool.execute(myTask);
    }
  }

  public static class Task implements Runnable {
    protected String name;

    public Task(String name) {
      super();
      this.name = name;
    }

    @Override
    public void run() {
      try {
        System.out.println(this.name + " is running."+" now time="+ LocalDateTime.now());
        Thread.sleep(1000*5);
//        Thread.sleep(500);
      } catch (Exception e) {
      }
    }
  }
}
