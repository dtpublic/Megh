package com.datatorrent.alerts;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public interface AlertReceiverInterface extends InputOperator
{
  DefaultOutputPort<Message> getMessageOutPort();
}
