package com.datatorrent.alerts.notification.email;

import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.alerts.ActionTuple;

public class EmailNotificationHandler implements ActionHandler{
  protected EmailTaskManager emailTaskManager = new EmailTaskManager();
  
  @Override
  public void handle(ActionTuple tuple) {
    sendEmail( tuple );
  }
  
  protected void sendEmail(ActionTuple tuple)
  {
    emailTaskManager.sendEmail(tuple);
  }
}
