package com.datatorrent.alerts.conf;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.alerts.notification.email.EmailNotificationContext;
import com.datatorrent.alerts.notification.email.EmailNotificationMessage;

public class BuildinEmailNotificationConfigRepo extends EmailNotificationConfigRepo {
  
  private static BuildinEmailNotificationConfigRepo instance = null;
  
  public static BuildinEmailNotificationConfigRepo instance()
  {
    if( instance == null )
    {
      synchronized(BuildinEmailNotificationConfigRepo.class)
      {
        if( instance == null )
        {
          instance = new BuildinEmailNotificationConfigRepo();
          instance.loadConfig();
        }
      }
    }
    return instance;
  }
  
  
  @Override
  public void loadConfig() {
    {
      Map<Integer, EmailNotificationContext> levelMap = new HashMap<Integer, EmailNotificationContext>();
      levelMap.put(EmailNotificationConfigRepo.ANY_LEVEL, new EmailNotificationContext( "smtp.gmail.com", 587, "bright@datatorrent", "password".toCharArray(), true ) );
      contextMap.put(EmailNotificationConfigRepo.ANY_APP, levelMap);
    }
    
    {
      Map<Integer, EmailNotificationMessage> levelMap = new HashMap<Integer, EmailNotificationMessage>();
      levelMap.put(EmailNotificationConfigRepo.ANY_LEVEL, new EmailNotificationMessage( new String[]{""}, null, null, "notification", "content"  ) );
      messageMap.put(EmailNotificationConfigRepo.ANY_APP, levelMap);
    }
    
  }

}
