package com.datatorrent.alerts.notification.email;

import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.alerts.ActionTuple;

public class EmailNotificationHandler implements ActionHandler<EmailNotificationTuple>{
  protected EmailTaskManager emailTaskManager = new EmailTaskManager();
  
  @Override
  public void handle(EmailNotificationTuple tuple) {
    sendEmail( tuple );
  }
  
  protected void sendEmail(EmailNotificationTuple tuple)
  {
    emailTaskManager.sendEmail(tuple);
  }
}
