package com.datatorrent.alerts.notification.email;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.alerts.ActionHandler;
import com.datatorrent.alerts.conf.DefaultEmailConfigRepo;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * @displayName Alerts Email Operator
 * @category Alerts
 * @tags alerts, email
 * @since 3.1.0
 */
@Evolving
public class EmailNotificationOperator extends BaseOperator{

  private transient ActionHandler<EmailNotificationTuple> actionHandler = new EmailNotificationHandler();
  
  public EmailNotificationOperator(String filePath)
  {
    setAlertsEmailConfFile(filePath);
  }
  
  public EmailNotificationOperator()
  {
  }
  
  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<EmailNotificationTuple> input = new DefaultInputPort<EmailNotificationTuple>()
  {
    @Override
    public void process(EmailNotificationTuple t)
    {
      processTuple(t);
    }

  };
  
  public void setAlertsEmailConfFile(String filePath)
  {
    System.setProperty(DefaultEmailConfigRepo.PROP_ALERTS_EMAIL_CONF_FILE, filePath);
  }
  
  public void processTuple(EmailNotificationTuple tuple)
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
