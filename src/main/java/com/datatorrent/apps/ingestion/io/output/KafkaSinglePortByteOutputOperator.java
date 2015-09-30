package com.datatorrent.apps.ingestion.io.output;

import kafka.producer.KeyedMessage;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.contrib.kafka.AbstractKafkaOutputOperator;

public class KafkaSinglePortByteOutputOperator extends AbstractKafkaOutputOperator<String, byte[]>
{
  @AutoMetric
  private long outputMessagesPerSec;
  
  @AutoMetric
  private long outputBytesPerSec;
  
  private long messageCount;
  private long byteCount;
  private double windowTimeSec; 

  public final transient DefaultInputPort<byte[]> inputPort = new DefaultInputPort<byte[]>() {
    @Override
    public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };

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

  protected void processTuple(byte[] tuple)
  {
    getProducer().send(new KeyedMessage<String, byte[]>(getTopic(), tuple));
    messageCount++;
    byteCount += tuple.length;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    outputBytesPerSec = (long) (byteCount / windowTimeSec);
    outputMessagesPerSec = (long) (messageCount / windowTimeSec);
  }
}
