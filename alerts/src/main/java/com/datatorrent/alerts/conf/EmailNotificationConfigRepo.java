package com.datatorrent.alerts.conf;

import java.util.Map;

import com.datatorrent.alerts.notification.email.EmailNotificationContext;
import com.datatorrent.alerts.notification.email.EmailNotificationMessage;
import com.datatorrent.common.util.Pair;

/**
 * make sure the loadConfig() only execute in one thread, and all other operations are read-only to avoid lock/unlock
 */
public abstract class EmailNotificationConfigRepo {
  public static final String ANY_APP = "";
//public static final String ANY_TOPIC = "";
  public static final int ANY_LEVEL = -1;
  

  protected Map<String, Map<Integer, EmailNotificationContext> > contextMap;
  protected Map<String, Map<Integer, EmailNotificationMessage> > messageMap;
  

  public abstract void loadConfig();

  
  public Pair<EmailNotificationContext, EmailNotificationMessage> getEmailNotification( String appName, int level )
  {
    EmailNotificationContext context = null;
    {
      Map<Integer, EmailNotificationContext> levelMap = contextMap.get(appName);
      if(levelMap == null)
        levelMap = contextMap.get(ANY_APP);
      if(levelMap == null)
        throw new AlertsConfigException( "Can't find email context." );
      context = levelMap.get(level);
      if(context == null)
        context = levelMap.get(ANY_LEVEL);
      if(context == null)
        throw new AlertsConfigException( "Can't find email context." );
    }
    
    EmailNotificationMessage message = null;
    {
      Map<Integer, EmailNotificationMessage> levelMap = messageMap.get(appName);
      if(levelMap == null)
        levelMap = messageMap.get(ANY_APP);
      if(levelMap == null)
        throw new AlertsConfigException( "Can't find email message." );
      message = levelMap.get(level);
      if(message == null)
        message = levelMap.get(ANY_LEVEL);
      if(message == null)
        throw new AlertsConfigException( "Can't find email message." );
    }
        
    return new Pair<EmailNotificationContext, EmailNotificationMessage>( context, message );
  }
}
