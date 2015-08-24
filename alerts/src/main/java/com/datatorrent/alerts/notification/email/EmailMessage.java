package com.datatorrent.alerts.notification.email;

public final class EmailMessage {
  protected final String subject;
  protected final String content;

  public EmailMessage(String subject, String content) {
    this.subject = subject;
    this.content = content;
  }
  
  @Override
  public String toString()
  {
    return String.format("subject: %s\ncontent: %s\n", subject, content);
  }
}
