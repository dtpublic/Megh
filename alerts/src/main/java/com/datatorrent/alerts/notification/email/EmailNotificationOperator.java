package com.datatorrent.alerts.notification.email;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.alerts.ActionTuple;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class EmailNotificationOperator extends BaseOperator{

  private ActionHandler actionHandler = new EmailNotificationHandler();
  
  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ActionTuple> input = new DefaultInputPort<ActionTuple>()
  {
    @Override
    public void process(ActionTuple t)
    {
      processTuple(t);
    }

  };
  
  
  public void processTuple(ActionTuple tuple)
  {
    actionHandler.handle(tuple);
  }
  

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }
  

  @Override
  public void teardown()
  {
  }
}
