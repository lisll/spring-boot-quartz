package com.datapipeline.springbootquartz.bootquartz;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import org.quartz.CronExpression;
import org.quartz.impl.triggers.CronTriggerImpl;

public class Demo {

  public static void main(String[] args) throws ParseException {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    LocalDateTime localDateTime = LocalDateTime.of(2023, 05, 17, 14, 32, 56);
    Instant instant = localDateTime.toInstant(ZoneOffset.of("+8"));
    String format = simpleDateFormat.format(Date.from(instant));
    CronExpression cronExpression = new CronExpression("50 * * * * ?");
    Date timeAfter = cronExpression.getTimeAfter(Date.from(instant));
    System.out.println(timeAfter);
  }

}
