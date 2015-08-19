package com.datatorrent.alerts.notification;

public interface Notification< C extends NotificationContext, M extends NotificationMessage >{
  public  void notify( C context, M message);
}
