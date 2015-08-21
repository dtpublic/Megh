package com.datatorrent.alerts;

import org.junit.Test;

import com.datatorrent.alerts.notification.email.EmailNotificationHandler;

public class SendEmailTester {
  
  protected String[] apps = new String[]{ "app1", "app2" };
  protected Integer[] levels = new Integer[]{ 1, 2, 3, 4 };
  
  @Test
  public void testSendEmail()
  {
    EmailNotificationHandler handler = new EmailNotificationHandler();
    
    for( String app : apps )
    {
      for( Integer level : levels )
      {
        handler.handle( getActionTuple( app, level ) );
      }
    }
  }
  
  public ActionTuple getActionTuple( String app, Integer level )
  {
    ActionTuple tuple = new ActionTuple();
    tuple.setAppName(app);
    tuple.setLevel(level);
    return tuple;
  }
}
