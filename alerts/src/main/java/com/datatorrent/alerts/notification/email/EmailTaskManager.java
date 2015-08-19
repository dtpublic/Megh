package com.datatorrent.alerts.notification.email;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.datatorrent.alerts.notification.Notification;

public class EmailTaskManager implements Notification<EmailNotificationContext, EmailNotificationMessage> {
  

  public static class EmailNotificationTask implements Runnable
  {
    private final EmailNotificationContext context;
    private final  EmailNotificationMessage message;
    public EmailNotificationTask(EmailNotificationContext context, EmailNotificationMessage message)
    {
      this.context = context;
      this.message = message;
    }
    
    @Override
    public void run() {
      emailNotification.notify(context, message);
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
  @Override
  public void notify(EmailNotificationContext context, EmailNotificationMessage message) {
    taskExecutor.execute( new EmailNotificationTask(context, message) );
  }

  
}
