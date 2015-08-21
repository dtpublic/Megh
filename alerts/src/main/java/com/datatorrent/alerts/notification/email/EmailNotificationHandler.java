package com.datatorrent.alerts.notification.email;

import java.util.List;

import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.alerts.ActionTuple;
import com.datatorrent.alerts.conf.DefaultEmailConfigRepo;

public class EmailNotificationHandler implements ActionHandler{
  protected EmailTaskManager emailTaskManager = new EmailTaskManager();
  
  @Override
  public void handle(ActionTuple tuple) {
    EmailInfo emailInfo = getEmailInfo( tuple );
    List<EmailInfo> emailInfos = DefaultEmailConfigRepo.instance().fillEmailInfo(tuple.getAppName(), tuple.getLevel(), emailInfo);
    for(EmailInfo ei : emailInfos)
      sendEmail( ei );
  }
  
  protected void sendEmail( EmailInfo emailInfo )
  {
    emailTaskManager.notify(emailInfo);
  }

  protected EmailInfo getEmailInfo( ActionTuple tuple )
  {
    EmailInfo emailInfo = new EmailInfo();
    return emailInfo;
  }
}
