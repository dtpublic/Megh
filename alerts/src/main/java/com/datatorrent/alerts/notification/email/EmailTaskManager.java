package com.datatorrent.alerts.notification.email;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.ActionTuple;
import com.datatorrent.alerts.conf.DefaultEmailConfigRepo;

public class EmailTaskManager {
  private static final Logger logger = LoggerFactory.getLogger(EmailTaskManager.class);
      
  public static class SendEmailTask implements Runnable
  {
    private final ActionTuple tuple;
    public SendEmailTask(ActionTuple tuple)
    {
      this.tuple = tuple;
    }
    
    @Override
    public void run() {

      EmailInfo emailInfo = getEmailInfo( tuple );
      List<EmailInfo> emailInfos = DefaultEmailConfigRepo.instance().fillEmailInfo(tuple.getAppName(), tuple.getLevel(), emailInfo);
      if(emailInfos == null || emailInfos.isEmpty())
      {
        logger.warn("Can't send email for action tuple: {}", tuple);
        return;
      }
      
      for(EmailInfo ei : emailInfos)
        emailNotification.sendEmail(ei);
    }
    
    protected EmailInfo getEmailInfo( ActionTuple tuple )
    {
      return new EmailInfo();
    }
  }
  
  
  
  public static final int CORE_POOL_SIZE = 10;
  public static final int MAX_POOL_SIZE = 200;
  public static final int ALIVE_SECONDS = 600;
  public static final int TASK_QUEUE_SIZE = MAX_POOL_SIZE;
  
  private ThreadPoolExecutor taskExecutor;
  protected static final EmailDeliver emailNotification = new EmailDeliver();
  
  public EmailTaskManager()
  {
    //load the configure here.
    DefaultEmailConfigRepo.instance();
    
    taskExecutor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, ALIVE_SECONDS, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(TASK_QUEUE_SIZE));
  }

  
  /**
   * should only one thread call this method.
   */
  public void sendEmail(ActionTuple tuple) {
    taskExecutor.execute(new SendEmailTask(tuple) );
  }
}
