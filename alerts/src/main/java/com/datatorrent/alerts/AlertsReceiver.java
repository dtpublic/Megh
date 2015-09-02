package com.datatorrent.alerts;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.rabbitmq.AbstractRabbitMQInputOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * @since 2.1.0
 */
public class AlertsReceiver extends AbstractRabbitMQInputOperator<Message> implements AlertReceiverInterface
{
  private final KryoSerializableStreamCodec<Message> codec = new KryoSerializableStreamCodec<Message>();
  final public transient DefaultOutputPort<Message> messageOutput = new DefaultOutputPort<Message>();

  @Override
  public void emitTuple(byte[] arg0)
  {
    Slice slice = new Slice(arg0);
    messageOutput.emit((Message)codec.fromByteArray(slice));
  }

  @Override
  public DefaultOutputPort<Message> getMessageOutPort()
  {
    return messageOutput;
  }
  
    /*private Integer i = 0 ;

    @Override
    public void emitTuples()
    {
        AlertMessage message = new AlertMessage() ;

        message.setFlag(true);
        ++i ;
        message.setEventId(i);
        message.setLevel(1) ;
        message.setAppId(i.toString());

        messageOutput.emit(message);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void beginWindow(long l)
    {

    }

    @Override
    public void endWindow()
    {

    }

    public final transient DefaultOutputPort<AlertMessage> messageOutput = new DefaultOutputPort<AlertMessage>();

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }*/
}
