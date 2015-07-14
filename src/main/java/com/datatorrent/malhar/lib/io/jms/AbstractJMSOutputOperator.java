package com.datatorrent.malhar.lib.io.jms;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This is the base implementation of an JMS output operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * This operator receives tuples from Malhar Streaming Platform through its input ports.
 * When the tuple is available in input ports it converts that to JMS message and send into
 * a message bus. The concrete class of this has to implement the abstract method
 * how to convert tuple into JMS message.
 * </p>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * </p>
 * @displayName Abstract JMS Output
 * @category Messaging
 * @tags jms, output operator
 *
 * @since 0.3.2
 */
public abstract class AbstractJMSOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractJMSOutputOperator.class);

  /**
   * Use this field to getStore() tuples from which messages are created.
   */
  private List<Object> tupleBatch = Lists.newArrayList();
  /**
   * Use this field to getStore() messages to be sent in batch.
   */
  private List<Message> messageBatch = Lists.newArrayList();

  private transient String appId;
  private transient int operatorId;
  private transient long committedWindowId;
  /**
   * The id of the current window.
   * Note this is not transient to handle the case that the operator restarts from a checkpoint that is
   * in the middle of the application window.
   */
  private long currentWindowId;
  private Operator.ProcessingMode mode;

  private transient MessageProducer producer;
  protected JMSBaseTransactionableStore store = new JMSTransactionableStore();

  private final JMSBase base = new JMSBase();

  @Override
  public void setup(Context.OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();

    logger.debug("Application Id {} operatorId {}", appId, operatorId);

    store.setBase(base);
    store.setAppId(appId);
    store.setOperatorId(operatorId);
    base.transacted = store.isTransactable();

    try {
      createConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
      throw new RuntimeException(ex);
    }

    logger.debug("Session is null {}:", base.getSession() == null);

    try {
      store.connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Done connecting store.");

    mode = context.getValue(Context.OperatorContext.PROCESSING_MODE);

    if(mode== Operator.ProcessingMode.AT_MOST_ONCE){
      //Batch must be cleared to avoid writing same data twice
      tupleBatch.clear();
    }

    for (Object tempObject: this.tupleBatch) {
      messageBatch.add(createMessage(tempObject));
    }

    committedWindowId = store.getCommittedWindowId(appId, operatorId);
    logger.debug("committedWindowId {}", committedWindowId);
    logger.debug("End of setup store in transaction: {}", store.isInTransaction());
  }

  @Override
  public void teardown()
  {
    tupleBatch.clear();
    messageBatch.clear();

    logger.debug("beginning teardown");
    try {
      store.disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    cleanup();

    logger.debug("ending teardown");
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    store.beginTransaction();
    logger.debug("Transaction started for window {}", windowId);
  }

  @Override
  public void endWindow()
  {
    logger.debug("Ending window {}", currentWindowId);

    if(store.isExactlyOnce()) {
      //Store committed window and data in same transaction
      if (committedWindowId < currentWindowId) {
        store.storeCommittedWindowId(appId, operatorId, currentWindowId);
        committedWindowId = currentWindowId;
      }

      flushBatch();
      store.commitTransaction();
    }
    else {
      //For transactionable stores which cannot support exactly once, At least
      //once can be insured by for storing the data and then the committed window
      //id.
      flushBatch();
      store.commitTransaction();

      if (committedWindowId < currentWindowId) {
        store.storeCommittedWindowId(appId, operatorId, currentWindowId);
        committedWindowId = currentWindowId;
      }
    }

    logger.debug("done ending window {}", currentWindowId);
  }

  /**
   * This is a helper method which flushes all the batched data.
   */
  protected void flushBatch()
  {
    logger.debug("flushing batch, batch size {}", tupleBatch.size());

    for (Message message: messageBatch) {
      try {
        producer.send(message);
      }
      catch (JMSException ex) {
        throw new RuntimeException(ex);
      }
    }

    tupleBatch.clear();
    messageBatch.clear();

    logger.debug("done flushing batch");
  }

  /**
   * This is a helper method which should be called to send a message.
   * @param data The data which will be converted into a message.
   */
  protected void sendMessage(Object data)
  {
    if(currentWindowId <= committedWindowId) {
      return;
    }

    tupleBatch.add(data);
    Message message = createMessage(data);
    messageBatch.add(message);

    if (tupleBatch.size() >= base.getBatch()) {
      flushBatch();
    }
  }

  public void setStore(JMSBaseTransactionableStore store)
  {
    this.store = store;
  }

  public JMSBaseTransactionableStore getStore()
  {
    return store;
  }

  /**
   *  Release resources.
   */
  public void cleanup()
  {
    try {
      producer.close();
      producer = null;

      base.cleanup();
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }

  /**
   *  Connection specific setup for JMS service.
   *
   *  @throws JMSException
   */
  public void createConnection() throws JMSException
  {
    base.createConnection();
    // Create producer
    producer = base.getSession().createProducer(base.getDestination());
  }

  /**
   * Convert tuple into JMS message. Tuple can be any Java Object.
   * @param tuple
   * @return Message
   */
  protected abstract Message createMessage(Object tuple);

  public Map<String, String> getConnectionFactoryProperties()
  {
    return base.getConnectionFactoryProperties();
  }

  public void setConnectionFactoryProperties(Map<String, String> connectionFactoryProperties)
  {
    base.setConnectionFactoryProperties(connectionFactoryProperties);
  }
  /**
   * @return the message acknowledgment mode
   */
  public String getAckMode()
  {
    return base.getAckMode();
  }

  /**
   * Sets the message acknowledgment mode.
   *
   * @param ackMode the message acknowledgment mode to set
   */
  public void setAckMode(String ackMode)
  {
    base.setAckMode(ackMode);
  }


  /**
   * @return the name of the destination
   */
  public String getSubject()
  {
    return base.getSubject();
  }

  /**
   * Sets the name of the destination.
   *
   * @param subject the name of the destination to set
   */
  public void setSubject(String subject)
  {
    base.setSubject(subject);
  }

  /**
   * @return the session
   */
  public Session getSession()
  {
    return base.getSession();
  }

  public void setTopic(boolean topic)
  {
    base.setTopic(topic);
  }

  /**
   * Sets the verbose option.
   *
   * @param verbose the flag to set to enable verbose option
   */
  public void setVerbose(boolean verbose)
  {
    base.setVerbose(verbose);
  }

  /**
   * Sets the client id.
   *
   * @param clientId the id to set for the client
   */
  public void setClientId(String clientId)
  {
    base.setClientId(clientId);
  }
  /**
   * Sets the batch for the JMS operator. JMS can acknowledge receipt
   * of messages back to the broker in batches (to improve performance).
   *
   * @param batch the size of the batch
   */
  public void setBatch(int batch)
  {
    base.setBatch(batch);
  }

  /**
   * Sets the size of the message.
   *
   * @param messageSize the size of the message
   */
  public void setMessageSize(int messageSize)
  {
    base.setMessageSize(messageSize);
  }
  /**
   * Sets the durability feature. Durable queues keep messages around persistently
   * for any suitable consumer to consume them.
   *
   * @param durable the flag to set to the durability feature
   */
  public void setDurable(boolean durable)
  {
    base.setDurable(durable);
  }
}
