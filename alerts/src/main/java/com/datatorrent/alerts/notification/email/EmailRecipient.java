package com.datatorrent.alerts.notification.email;

public class EmailRecipient {
  protected final String[] to;
  protected final String[] cc;
  protected final String[] bcc;
  
  public EmailRecipient( String[] to, String[] cc, String[] bcc )
  {
    this.to = to;
    this.cc = cc;
    this.bcc = bcc;
  }
}
