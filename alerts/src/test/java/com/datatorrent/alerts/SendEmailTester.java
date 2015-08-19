package com.datatorrent.alerts;

import org.junit.Test;

import com.datatorrent.alerts.notification.email.EmailNotification;
import com.datatorrent.alerts.notification.email.EmailNotificationContext;
import com.datatorrent.alerts.notification.email.EmailNotificationMessage;

public class SendEmailTester {
  
  @Test
  public void testSendEmail()
  {
    EmailNotification notification = new EmailNotification();
    
    EmailNotificationContext context = new EmailNotificationContext( "smtp.gmail.com", 587, "bright@datatorrent.com", "password".toCharArray(), true );
    EmailNotificationMessage message = new EmailNotificationMessage( new String[]{"bright@datatorrent.com"}, null, null, "email notification", "something wrong" );
    notification.notify(context, message);
    
  }
}
