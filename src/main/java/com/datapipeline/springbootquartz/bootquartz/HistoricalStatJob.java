package com.datapipeline.springbootquartz.bootquartz;

import java.time.LocalDateTime;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.TriggerBuilder;

public class HistoricalStatJob extends AbstractWebServiceJob {

  String cron = "30 * * * * ?";

  @Override
  protected JobInfo jobInfo() {
    var jobDetail =
        JobBuilder.newJob(getClass())
            .withIdentity("historicalJob", SERVICE_GROUP)
            .storeDurably(true)
            .withDescription("this is historicalJob")
            .requestRecovery(true)
            .build();

    CronScheduleBuilder schedBuilder = CronScheduleBuilder.cronSchedule(cron);
    var historicalTrigger =
        TriggerBuilder.newTrigger()
            .forJob(jobDetail)
            .withIdentity("historicalTrigger", SERVICE_GROUP)
            .withSchedule(schedBuilder.inTimeZone(TIME_ZONE))
            .build();
    return null;
  }

  @Override
  protected void executeJob(JobExecutionContext context) throws Exception {
    System.out.println(Thread.currentThread().getName()+" 任务开始执行 "+ LocalDateTime.now());
  }
}
