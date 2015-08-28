package com.datatorrent.alerts.notification.email;

import java.util.Collection;
import java.util.List;

import com.datatorrent.alerts.ActionTuple;

public class EmailNotificationTuple extends ActionTuple{
  public EmailNotificationTuple()
  {
    this.setAction(Action.NOTIFY_EMAIL);
  }
  
  protected Collection<String> tos;
  protected Collection<String> ccs;
  protected Collection<String> bccs;
  
  protected String subject;
  protected String content;
  

  public Collection<String> getTos() {
    return tos;
  }
  public void setTos(Collection<String> tos) {
    this.tos = tos;
  }
  public Collection<String> getCcs() {
    return ccs;
  }
  public void setCcs(Collection<String> ccs) {
    this.ccs = ccs;
  }
  public Collection<String> getBccs() {
    return bccs;
  }
  public void setBccs(Collection<String> bccs) {
    this.bccs = bccs;
  }
  public String getSubject() {
    return subject;
  }
  public void setSubject(String subject) {
    this.subject = subject;
  }
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }
  
  
}
