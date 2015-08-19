package com.datatorrent.alerts.notification.email;

import com.datatorrent.alerts.notification.NotificationMessage;

public final class EmailNotificationMessage extends NotificationMessage {

  protected final String[] to;
  protected final String[] cc;
  protected final String[] bcc;
  protected final String subject;
  protected final String content;
  
  public EmailNotificationMessage( String[] to, String[] cc, String[] bcc, String subject, String content )
  {
    this.to = to;
    this.cc = cc;
    this.bcc = bcc;
    this.subject = subject;
    this.content = content;
  }
}
