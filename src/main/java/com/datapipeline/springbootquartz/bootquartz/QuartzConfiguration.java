package com.datapipeline.springbootquartz.bootquartz;

import static org.springframework.beans.factory.config.AutowireCapableBeanFactory.AUTOWIRE_CONSTRUCTOR;

import com.datapipeline.springbootquartz.bootquartz.AbstractWebServiceJob.JobInfo;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.Trigger;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.quartz.LocalDataSourceJobStore;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;
import org.springframework.util.ReflectionUtils;

@Configuration
public class QuartzConfiguration {

  private static final RejectedExecutionHandler REJECTED_EXECUTION_HANDLER = new CallerRunsPolicy();

  @Autowired public DataSource dataSource;

  @Bean(value = "webserviceQuartzScheduler", destroyMethod = "destroy")
  public SchedulerFactoryBean quartzScheduler(
      ApplicationContext appContext, @Qualifier("springbootQuartzExecutor") Executor taskExecutor)
      throws Exception {
    var provider =
        new ClassPathScanningCandidateComponentProvider(false, appContext.getEnvironment());
    var components = provider.findCandidateComponents(AbstractWebServiceJob.class.getPackageName());
    var jobInfos = new ArrayList<JobInfo>(components.size());
    var beanFactory = appContext.getAutowireCapableBeanFactory();
    var cl = Thread.currentThread().getContextClassLoader();
    for (var component : components) {
      var clazz = Class.forName(component.getBeanClassName(), false, cl);
      var bean = beanFactory.createBean(clazz, AUTOWIRE_CONSTRUCTOR, false);
      var jobInfo = ((AbstractWebServiceJob) bean).jobInfo();
      jobInfos.add(jobInfo);
      beanFactory.destroyBean(bean);
    }
    var jobEntry =
        jobInfos.stream()
            .collect(
                Collectors.teeing(
                    Collectors.mapping(JobInfo::jobDetail, Collectors.toList()),
                    Collectors.mapping(JobInfo::trigger, Collectors.toList()),
                    (e1, e2) ->
                        Map.entry(e1.toArray(JobDetail[]::new), e2.toArray(Trigger[]::new))));
    var jobFactory = new SpringBeanJobFactory();
    jobFactory.setApplicationContext(appContext);
    var schedulerFactoryBean = new SchedulerFactoryBean();
    schedulerFactoryBean.setApplicationContext(appContext);
    schedulerFactoryBean.setJobFactory(jobFactory);
    schedulerFactoryBean.setSchedulerName("springbootQuartz");
    schedulerFactoryBean.setApplicationContextSchedulerContextKey("springApplicationContext");
    schedulerFactoryBean.setAutoStartup(true);
    schedulerFactoryBean.setStartupDelay(1);
    schedulerFactoryBean.setExposeSchedulerInRepository(false);
    schedulerFactoryBean.setWaitForJobsToCompleteOnShutdown(false);
    schedulerFactoryBean.setOverwriteExistingJobs(true);
    schedulerFactoryBean.setDataSource(dataSource);
    schedulerFactoryBean.setJobDetails(jobEntry.getKey());
    schedulerFactoryBean.setTriggers(jobEntry.getValue());
    schedulerFactoryBean.setTaskExecutor(taskExecutor);
    var quartzProperties = new Properties(20);
    schedulerFactoryBean.setQuartzProperties(quartzProperties);
    JobDetail[] jobDetail = jobEntry.getKey();
    final AtomicInteger count = new AtomicInteger(1);
    for (JobDetail detail : jobDetail) {
      System.out.println(this.getClass() + "->" + detail.getKey());
      JobKey key = detail.getKey();
      if (key.toString().contains("HistoricalStatJob")) {
        detail.getJobDataMap().put("HistoricalStatJob_count", count);
        System.out.println("JobKey----->" + key);
      }
    }

//    quartzProperties.setProperty("org.quartz.jobStore.class", WebQuartzJobStore.class.getName());

    quartzProperties.setProperty("org.quartz.scheduler.instanceName", "webservice-jobs");
    quartzProperties.setProperty("org.quartz.scheduler.instanceId", "AUTO");
    quartzProperties.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
    quartzProperties.setProperty("org.quartz.scheduler.idleWaitTime", "60000");
    quartzProperties.setProperty("org.quartz.scheduler.makeSchedulerThreadDaemon", "true");
    quartzProperties.setProperty("org.quartz.scheduler.dbFailureRetryInterval", "5000");
    quartzProperties.setProperty(
        "org.quartz.scheduler.org.quartz.scheduler.threadsInheritContextClassLoaderOfInitializer",
        "true");
    quartzProperties.setProperty("org.quartz.jobStore.useProperties", "false");
    quartzProperties.setProperty(
        "org.quartz.jobStore.driverDelegateClass", "org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
    quartzProperties.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
//    quartzProperties.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
    quartzProperties.setProperty("org.quartz.jobStore.isClustered", "true");
    quartzProperties.setProperty("org.quartz.jobStore.acquireTriggersWithinLock", "true");
    quartzProperties.setProperty(
        "org.quartz.plugin.shutdownHook.class", "org.quartz.plugins.management.ShutdownHookPlugin");
    quartzProperties.setProperty("org.quartz.plugin.shutdownHook.cleanShutdown", "true");
    return schedulerFactoryBean;
  }

  @Bean(name = "springbootQuartzExecutor")
  public TaskExecutor asyncExecutor() {
    var size = Runtime.getRuntime().availableProcessors();
    var coreSize = Math.max(5, size / 3);
    return new TaskExecutorBuilder()
        .corePoolSize(coreSize)
        .maxPoolSize(Math.max(size, coreSize))
        .keepAlive(Duration.ofSeconds(120L))
        .threadNamePrefix("springboot-quartz")
        .taskDecorator(
            runnable ->
                () -> {
                  try {
                    runnable.run();
                  } catch (Exception ex) {
                    System.out.println(ex);
                  }
                })
        .additionalCustomizers(
            taskExecutor -> {
              taskExecutor.setRejectedExecutionHandler(REJECTED_EXECUTION_HANDLER);
              taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
            })
        .build();
  }

  public static class WebQuartzJobStore extends LocalDataSourceJobStore {

    private DataSource ds;

    @Override
    @SuppressWarnings("NullableProblems")
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
        throws SchedulerConfigException {
      super.initialize(loadHelper, signaler);
      try {
        var f = ReflectionUtils.findField(this.getClass(), "dataSource");
        ReflectionUtils.makeAccessible(Objects.requireNonNull(f));
        this.ds = (DataSource) ReflectionUtils.getField(f, this);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected <T> T executeInNonManagedTXLock(
        String lockName, TransactionCallback<T> txCallback, TransactionValidator<T> txValidator)
        throws JobPersistenceException {
      Connection conn = null;
      try {
        var transOwner = false;
        try {
          if (lockName != null) {
            // If we aren't using db locks, then delay getting DB connection
            // until after acquiring the lock since it isn't needed.
            if (getLockHandler().requiresConnection()) {
              conn = getNonManagedTXConnection();
            }
            transOwner = getLockHandler().obtainLock(conn, lockName);
          }

          if (conn == null) {
            conn = getNonManagedTXConnection();
          }

          var result = txCallback.execute(conn);
          try {
            commitConnection(conn);
          } catch (JobPersistenceException e) {
            rollbackConnection(conn);
            if (txValidator == null
                || !retryExecuteInNonManagedTXLock(
                    lockName, conn1 -> txValidator.validate(conn1, result))) {
              throw e;
            }
          }

          var sigTime = clearAndGetSignalSchedulingChangeOnTxCompletion();
          if (sigTime != null && sigTime >= 0) {
            signalSchedulingChangeImmediately(sigTime);
          }

          return result;
        } catch (JobPersistenceException e) {
          rollbackConnection(conn);
          throw e;
        } catch (RuntimeException e) {
          rollbackConnection(conn);
          throw new JobPersistenceException("Unexpected runtime exception: " + e.getMessage(), e);
        } finally {
          try {
            releaseLock(lockName, transOwner);
          } finally {
            cleanupConnection(conn);
          }
        }
      } catch (JobPersistenceException ex) {
        throw invalid(ex, conn);
      }
    }

    @Override
    protected Object executeInLock(String lockName, TransactionCallback txCallback)
        throws JobPersistenceException {
      var transOwner = false;
      Connection conn = null;
      try {
        if (lockName != null) {
          // If we aren't using db locks, then delay getting DB connection
          // until after acquiring the lock since it isn't needed.
          if (getLockHandler().requiresConnection()) {
            conn = getConnection();
          }
          transOwner = getLockHandler().obtainLock(conn, lockName);
        }

        if (conn == null) {
          conn = getConnection();
        }

        return txCallback.execute(conn);
      } catch (JobPersistenceException ex) {
        throw invalid(ex, conn);
      } finally {
        try {
          releaseLock(lockName, transOwner);
        } finally {
          cleanupConnection(conn);
        }
      }
    }

    private JobPersistenceException invalid(JobPersistenceException ex, Connection conn)
        throws JobPersistenceException {
      return ex;
    }
  }
}
