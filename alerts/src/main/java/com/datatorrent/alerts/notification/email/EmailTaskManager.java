package com.datatorrent.alerts.notification.email;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EmailTaskManager {
  

  public static class EmailNotificationTask implements Runnable
  {
    private final EmailInfo emailInfo;
    public EmailNotificationTask(EmailInfo emailInfo)
    {
      this.emailInfo = emailInfo;
    }
    
    @Override
    public void run() {
      emailNotification.notify(emailInfo);
    }
  }
  
  
  
  public static final int CORE_POOL_SIZE = 10;
  public static final int MAX_POOL_SIZE = 200;
  public static final int ALIVE_SECONDS = 600;
  public static final int TASK_QUEUE_SIZE = MAX_POOL_SIZE;
  
  private ThreadPoolExecutor taskExecutor;
  protected static final EmailNotification emailNotification = new EmailNotification();
  
  public EmailTaskManager()
  {
    taskExecutor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, ALIVE_SECONDS, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(TASK_QUEUE_SIZE));
  }

  
  /**
   * should only one thread call this method.
   */
  public void notify(EmailInfo emailInfo) {
    taskExecutor.execute(new EmailNotificationTask(emailInfo) );
  }

  
}
