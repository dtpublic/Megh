package com.datatorrent.alerts;

import org.junit.Test;

import com.datatorrent.alerts.notification.email.EmailNotification;
import com.datatorrent.alerts.notification.email.EmailRecipient;
import com.datatorrent.alerts.notification.email.EmailContext;
import com.datatorrent.alerts.notification.email.EmailMessage;

public class SendEmailTester {
  
  @Test
  public void testSendEmail()
  {
    EmailNotification notification = new EmailNotification();
    
    EmailContext context = new EmailContext( "smtp.gmail.com", 587, "bright@datatorrent.com", "password".toCharArray(), true );
    EmailRecipient recipient = new EmailRecipient(new String[]{"bright@datatorrent.com"}, null, null);
    EmailMessage message = new EmailMessage( "email notification", "something wrong" );
    notification.notify(context, message, recipient);
    
  }
}
