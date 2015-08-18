package com.datatorrent.apps.ingestion.io.jms;

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
      if (tuple instanceof Message) {
        return (Message)tuple;
      }
      else if (tuple instanceof String) {
        return getSession().createTextMessage((String)tuple);
      }
      else if (tuple instanceof byte[]) {
        BytesMessage message = getSession().createBytesMessage();
        message.writeBytes((byte[])tuple);
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
