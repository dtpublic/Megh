package com.datatorrent.alerts.notification.email;

import com.datatorrent.alerts.notification.NotificationContext;

public final class EmailNotificationContext extends NotificationContext{
  protected final String smtpServer;
  protected final int smtpPort;
  protected final String sender;
  protected final char[] password;    //set password to null if support anonymous
  protected final boolean enableTls;
  
  public EmailNotificationContext( String smtpServer, int smtpPort, String sender, char[] password, boolean enableTls )
  {
    this.smtpServer = smtpServer;
    this.smtpPort = smtpPort;
    this.sender = sender;
    this.password = password;
    this.enableTls = enableTls;
  }
}
