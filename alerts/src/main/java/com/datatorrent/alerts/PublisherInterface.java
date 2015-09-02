package com.datatorrent.alerts;

public interface PublisherInterface
{

  boolean publishAlert(Message alert);

  boolean configureAlertPublishing();

  void teardown();

}
