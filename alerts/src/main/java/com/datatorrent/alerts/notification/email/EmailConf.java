package com.datatorrent.alerts.notification.email;

public final class EmailConf {
  protected final EmailContext context;
  protected final EmailRecipient recipient;
  protected final EmailMessage message;
  
  public EmailConf(EmailContext context, EmailRecipient recipient, EmailMessage message )
  {
    this.context = context;
    this.recipient = recipient;
    this.message = message;
  }
}
