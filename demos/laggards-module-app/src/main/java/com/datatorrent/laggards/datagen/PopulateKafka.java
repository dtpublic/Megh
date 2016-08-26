/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.demos.laggards.datagen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 */
public class PopulateKafka
{
  private static final Logger logger = LoggerFactory.getLogger(PopulateKafka.class);
  public static void main(String[] args) throws IOException, InterruptedException
  {
    AtomicInteger sid = new AtomicInteger();
    Properties config = new Properties();
    if (args.length >= 1) {
      config = getProperties(args[0]);
      logger.debug("Loading config from " + args[0]);
    }

    String topic = config.getProperty("topic", "laggards");
    int numThreads = Integer.parseInt(config.getProperty("threads", "5"));
    int maxCount = Integer.parseInt(config.getProperty("maxcount", "-1"));

    List<Thread> threads = new ArrayList<Thread>();

    for (int i = 0; i < numThreads; i++) {
      GeneratorThread gt = new GeneratorThread(topic, config, sid);
      threads.add(gt);
      gt.start();
    }

    while (true) {
      Thread.sleep(1000);
      logger.debug("Current value of seq " + sid.get());
      if ((maxCount != -1) && (sid.get() >= maxCount)) {
        break;
      }
    }

    logger.debug("terminated main while loop");
    for (Thread th : threads) {
      th.join();
    }
  }

  private static Properties getProperties(String configFilePath) throws IOException
  {
    Properties configProp = new Properties();
    BufferedReader br = new BufferedReader(new FileReader(configFilePath));
    configProp.load(br);
    br.close();
    return configProp;
  }

  static class GeneratorThread extends Thread
  {
    private final int maxCount;
    String topic;
    Properties config;
    AtomicInteger sid;
    Gson gson;

    Producer<String, String> createProducer()
    {
      Properties props = new Properties();
      logger.debug("broker list is " + config.getProperty("broker_list", "localhost:9092"));
      props.put("metadata.broker.list", config.getProperty("broker_list", "localhost:9092"));
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      //props.put("partitioner.class", SimplePartitioner.class.toString());
      props.put("request.required.acks", "1");
      ProducerConfig pconfig = new ProducerConfig(props);
      Producer producer = new Producer<String, String>(pconfig);
      return producer;
    }

    public GeneratorThread(String topic, Properties config, AtomicInteger sid)
    {
      this.topic = topic;
      this.config = config;
      this.sid = sid;
      this.maxCount = Integer.parseInt(config.getProperty("maxCount", "-1"));

      gson = new Gson();
    }

    @Override public void run()
    {
      Producer<String, String> producer = createProducer();
      logger.debug("value of maxCount is " + maxCount);
      long count = 0;
      int curr = 0;
      int rate = Integer.parseInt(config.getProperty("rate_per_thread", "20"));
      ObjectMapper mapper = new ObjectMapper();
      RateLimiter rt = null;
      if (rate != -1) {
        rt = RateLimiter.create(rate);
      }

      try {
        while (true) {
          for (int ctr = 0; ctr < 100; ctr++) {
            Random randomGenerator = new Random();
            // currnetTime + 2 mins - (1 hr + 15 mins + 5 mins)
            //long time = (System.currentTimeMillis() / 1000L) + 2*60 - randomGenerator.nextInt(4800);

            // currnetTime + 2 mins - (15 mins + 5 mins + 25mins)
            long time = (System.currentTimeMillis() / 1000L) + 2 * 60 - randomGenerator.nextInt(3000);
            String str = String.format("{\"time\": %d}", time);
            if (rt != null) {
              rt.acquire();
            }
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(count), str);
            producer.send(data);
            count++;
            curr = sid.incrementAndGet();
            if (maxCount != -1 && curr > maxCount) {
              break;
            }
          }
          if (maxCount != -1 && curr > maxCount) {
            break;
          }
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
