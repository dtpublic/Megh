package com.datatorrent.apps.ingestion.io.jms;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.io.jms.AbstractJMSOutputOperator;

import java.io.Serializable;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete class of JMS output operator.
 * createMessage API converts input tuple inot JMS message.
 *
 * @param <T>
 * @since 1.0.0
 */
public class JMSOutputOperator<T> extends AbstractJMSOutputOperator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(JMSOutputOperator.class);

  
  @AutoMetric
  private long outputMessagesPerSec;
  
  @AutoMetric
  private long outputBytesPerSec;
  
  private long messageCount;
  private long byteCount;
  private double windowTimeSec; 

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    outputMessagesPerSec = 0;
    outputBytesPerSec = 0;
    messageCount = 0;
    byteCount = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    outputBytesPerSec = (long) (byteCount / windowTimeSec);
    outputMessagesPerSec = (long) (messageCount / windowTimeSec);
  }

  /**
   * Convert to and send message.
   * @param tuple
   */
  protected void processTuple(T tuple)
  {
    sendMessage(tuple);
  }

  /**
   * This is an input port which receives tuples to be written out to an JMS message bus.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  @Override protected Message createMessage(Object tuple)
  {
    try {
      messageCount++;
      if (tuple instanceof Message) {
        return (Message)tuple;
      }
      else if (tuple instanceof String) {
        byteCount += ((String)tuple).length();
        return getSession().createTextMessage((String)tuple);
      }
      else if (tuple instanceof byte[]) {
        BytesMessage message = getSession().createBytesMessage();
        message.writeBytes((byte[])tuple);
        byteCount += ((byte[])tuple).length;
        return message;
      }
      else if (tuple instanceof Map) {
        return createMessageForMap((Map)tuple);
      }
      else if (tuple instanceof Serializable) {
        return getSession().createObjectMessage((Serializable)tuple);
      }
      else {
        throw new RuntimeException("Cannot convert object of type "
            + tuple.getClass() + "] to JMS message. Supported message "
            + "payloads are: String, byte array, Map<String,?>, Serializable object.");
      }
    }
    catch (JMSException ex) {
      logger.error(ex.getLocalizedMessage());
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create a JMS MapMessage for the given Map.
   *
   * @param map the Map to convert
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForMap(Map<?,?> map) throws JMSException
  {
    MapMessage message = getSession().createMapMessage();
    for (Map.Entry<?,?> entry: map.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        throw new RuntimeException("Cannot convert non-String key of type ["
            + entry.getKey().getClass() + "] to JMS MapMessage entry");
      }
      message.setObject((String)entry.getKey(), entry.getValue());
    }
    return message;
  }

}
