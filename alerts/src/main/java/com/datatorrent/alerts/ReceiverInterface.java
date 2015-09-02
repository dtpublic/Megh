package com.datatorrent.alerts;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public interface ReceiverInterface extends InputOperator
{
  DefaultOutputPort<Message> getMessageOutPort();
}
