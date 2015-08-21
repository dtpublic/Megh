package com.datatorrent.alerts.notification.email;

public final class EmailConf {
  protected EmailContext context;
  protected EmailRecipient recipient;
  protected EmailMessage message;

  public EmailConf() {
  }
  public EmailConf(EmailContext context, EmailRecipient recipient, EmailMessage message) 
  {
    setValue(context, recipient, message);
  }
  
  public void setValue(EmailContext context, EmailRecipient recipient, EmailMessage message) {
    this.context = context;
    this.recipient = recipient;
    this.message = message;
  }
  
}
