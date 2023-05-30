package com.datapipeline.springbootquartz.quartz;


import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdScheduler;
import org.quartz.impl.StdSchedulerFactory;

public class TestJob {

  public static void main(String[] args) {
    JobDetail jobDetail = JobBuilder.newJob(Myjob.class)
        .withIdentity("jobDeatil1","group1")
        .withDescription("this is test job")
        .storeDurably(true)
        .requestRecovery(true)
        .usingJobData("detailCount",1)
        .build();

    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity("trigger1","triggerGroup")
        .forJob(jobDetail)
        .startNow()
        .withSchedule(CronScheduleBuilder.cronSchedule("30 * * * * ?"))
        .usingJobData("triggerCount",1)
        .build();

    try {
      Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.scheduleJob(jobDetail,trigger);
      scheduler.start();
    } catch (SchedulerException e) {
      e.printStackTrace();
    }
  }
}
