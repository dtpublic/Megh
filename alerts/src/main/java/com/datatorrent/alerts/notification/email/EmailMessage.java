package com.datatorrent.alerts.notification.email;

public final class EmailMessage {
  protected final String subject;
  protected final String content;

  public EmailMessage(String subject, String content) {
    this.subject = subject;
    this.content = content;
  }
}
