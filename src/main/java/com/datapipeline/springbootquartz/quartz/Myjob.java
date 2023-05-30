package com.datapipeline.springbootquartz.quartz;



import java.time.LocalDateTime;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class Myjob implements Job {

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    int detailCount = context.getJobDetail().getJobDataMap().getInt("detailCount");
    int triggerCount = context.getTrigger().getJobDataMap().getInt("triggerCount");
    System.out.println(Thread.currentThread().getName()+" : execute --" + LocalDateTime.now()+" : detailCount="+detailCount+",triggerCount="+triggerCount);
    context.getJobDetail().getJobDataMap().put("detailCount", detailCount +1);
    System.out.println("now jobDetail->"+System.identityHashCode(context.getJobDetail()));
    System.out.println("now job->"+System.identityHashCode(context.getJobInstance()));
    try {
      Thread.sleep(1000*80);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
