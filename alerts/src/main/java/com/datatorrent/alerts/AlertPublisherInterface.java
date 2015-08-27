package com.datatorrent.alerts;

public interface AlertPublisherInterface
{
  boolean publishAlert(AlertAction alert);

  boolean configureAlertPublishing();

  void teardown();

}
