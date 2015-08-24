package com.datatorrent.alerts.notification.email;

public final class EmailContext{
  protected final String smtpServer;
  protected final int smtpPort;
  protected final String sender;
  protected final char[] password;    //set password to null if support anonymous
  protected final boolean enableTls;
  
  public EmailContext( String smtpServer, int smtpPort, String sender, char[] password, boolean enableTls )
  {
    this.smtpServer = smtpServer;
    this.smtpPort = smtpPort;
    this.sender = sender;
    this.password = password;
    this.enableTls = enableTls;
  }
  
  @Override
  public String toString()
  {
    return String.format("smtpServer: %s; smtpPort: %d; sender: %s; has Password: %b; enableTls: %b", 
        smtpServer, smtpPort, sender, Boolean.valueOf(password!=null), Boolean.valueOf(enableTls));
  }
}
