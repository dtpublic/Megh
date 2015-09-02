package com.datatorrent.alerts;

public interface AlertPublisherInterface
{
  boolean publishAlert(Message alert);

  boolean configureAlertPublishing();

  void teardown();

}
