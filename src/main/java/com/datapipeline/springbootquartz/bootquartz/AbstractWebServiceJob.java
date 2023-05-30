package com.datapipeline.springbootquartz.bootquartz;

import java.time.ZoneId;
import java.util.TimeZone;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.Trigger;
import org.springframework.scheduling.quartz.QuartzJobBean;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public abstract class AbstractWebServiceJob extends QuartzJobBean {

  static final String SERVICE_GROUP = "webservice-service-jobs";

  protected static final TimeZone TIME_ZONE = TimeZone.getTimeZone(ZoneId.of(ZoneId.SHORT_IDS.get("CTT")));

  @Override
  protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    var key = context.getJobDetail().getKey();
    var name = Thread.currentThread().getName();
    Thread.currentThread().setName("%s-%s".formatted(key.getName(), name));
    try {
      var identity = key.toString();
      System.out.println("start executing job " + identity);
      executeJob(context);
      System.out.println("end executing job"+identity);
    } catch (Exception ex) {

    }
  }

  protected abstract JobInfo jobInfo();

  protected abstract void executeJob(JobExecutionContext context) throws Exception;

  public record JobInfo(JobDetail jobDetail, Trigger trigger) {}
}
