//package com.datatorrent.apps.ingestion.io.kafka;
//
//import java.io.File;
//import java.util.List;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.datatorrent.api.LocalMode;
//import com.datatorrent.apps.ingestion.Application;
//import com.datatorrent.malhar.contrib.kafka.KafkaTestProducer;
//import com.datatorrent.malhar.contrib.kafka.KafkaOperatorTestBase;
//
//public class KafkaApplicationTest
//{
//  private static final Logger LOG = LoggerFactory.getLogger(KafkaApplicationTest.class);
//  private static final String KAFKA_TOPIC = "kafkaIngestionTest";
//  private static String OUTPUT_DIR;
//  private KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();
//
//  @Before
//  public void beforeTest() throws Exception
//  {
//    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
//    OUTPUT_DIR = kafkaLauncher.baseDir + File.separator + "output";
//    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
//
//    kafkaLauncher.startZookeeper();
//    kafkaLauncher.startKafkaServer();
//    kafkaLauncher.createTopic(0, KAFKA_TOPIC);
//  }
//
//  @After
//  public void afterTest()
//  {
//    kafkaLauncher.stopKafkaServer();
//    kafkaLauncher.stopZookeeper();
//  }
//
//  @Test
//  public void testApplication() throws Exception
//  {
//    LocalMode lma = LocalMode.newInstance();
//    Configuration conf = new Configuration(false);
//    conf.set("dt.operator.BlockReader.prop.scheme", "kafka");
//    conf.set("dt.output.protocol", "file");
//    conf.set("dt.operator.MessageReader.prop.zookeeper", "localhost:2182;localhost:2183");
//    conf.set("dt.operator.MessageReader.prop.topic", KAFKA_TOPIC);
//    conf.set("dt.operator.FileWriter.prop.filePath", OUTPUT_DIR);
//
//    lma.prepareDAG(new Application(), conf);
//    LocalMode.Controller lc = lma.getController();
//    lc.setHeartbeatMonitoringEnabled(false);
//
//    lc.runAsync();
//
//    KafkaTestProducer p = new KafkaTestProducer(KAFKA_TOPIC);
//    p.setSendCount(3);
//    new Thread(p).start();
//
//    long now = System.currentTimeMillis();
//    Path outDir = new Path(OUTPUT_DIR);
//    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
//    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 60000) {
//      Thread.sleep(10000);
//      LOG.debug("Waiting for {}", outDir);
//    }
//    Thread.sleep(5000);
//    lc.shutdown();
//    fs.close();
//
//    Assert.assertTrue("output dir does not exist", fs.exists(outDir));
//    File outputFile = new File(OUTPUT_DIR).listFiles()[0];
//    List<String> actual = FileUtils.readLines(outputFile);
//
//    Assert.assertEquals("Kafka tuple count not matching", 4, actual.size());
//    Assert.assertEquals("Kafka message not matching", "c1Message_1", actual.get(0));
//    Assert.assertEquals("Kafka message not matching", "c1Message_2", actual.get(1));
//    Assert.assertEquals("Kafka message not matching", "c1Message_3", actual.get(2));
//  }
//}
