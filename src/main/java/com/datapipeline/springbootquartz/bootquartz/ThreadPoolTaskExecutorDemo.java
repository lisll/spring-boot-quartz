package com.datapipeline.springbootquartz.bootquartz;

import java.time.LocalDateTime;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class ThreadPoolTaskExecutorDemo {

  public static void main(String[] args) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(150);
    threadPoolTaskExecutor.setMaxPoolSize(300);
    threadPoolTaskExecutor.setQueueCapacity(150);
    threadPoolTaskExecutor.initialize();

    int count = 0;
    while (count < 200) {
      threadPoolTaskExecutor.execute(() -> {
        System.out.println(Thread.currentThread().getName()+" , Execute start at "+LocalDateTime.now());
        try {
          Thread.sleep(2_000);
        } catch (InterruptedException e) {
          System.out.println(e);
          Thread.interrupted();
        }
        System.out.println("Execute end at "+ LocalDateTime.now());
      });

      count++;
    }
  }
}
