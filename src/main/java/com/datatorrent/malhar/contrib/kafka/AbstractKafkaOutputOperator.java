package com.datatorrent.malhar.contrib.kafka;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation of a Kafka output operator, which writes data to the Kafka message bus.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from has to implement createKafkaProducerConfig() to setup producer configuration.<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Abstract Kafka Output
 * @category Messaging
 * @tags output operator
 *
 * @since 0.3.2
 */
public abstract class AbstractKafkaOutputOperator<K, V> extends BaseOperator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaOutputOperator.class);
  private transient kafka.javaapi.producer.Producer<K, V> producer;  // K is key partitioner, V is value type
  @NotNull
  private String topic = "topic1";

  protected int sendCount;

  private String producerProperties = "";

  private Properties configProperties = new Properties();

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }


  /**
   * setup producer configuration.
   * @return ProducerConfig
   */
  protected ProducerConfig createKafkaProducerConfig(){
    Properties prop = new Properties();
    for (String propString : producerProperties.split(",")) {
      if (!propString.contains("=")) {
        continue;
      }
      String[] keyVal = StringUtils.trim(propString).split("=");
      prop.put(StringUtils.trim(keyVal[0]), StringUtils.trim(keyVal[1]));
    }

    configProperties.putAll(prop);

    return new ProducerConfig(configProperties);
  }

  public Producer<K, V> getProducer()
  {
    return producer;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    producer = new Producer<K, V>(createKafkaProducerConfig());
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    producer.close();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
