/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.LocalMode;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.lib.io.jms.JMSTestBase;

/**
 * This class creates TestApplication and runs it in localmode to test JMS ingestion.
 */
public class JMSApplicationTest
{

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;
    public String outputDirectory;
    String recoveryDir;
    private static final String BROKER_URL = "vm://localhost";
    private static final String SUBJECT = "TEST.FOO";

    JMSTestBase testBase;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.outputDirectory = baseDirectory + "/output";
      recoveryDir = baseDirectory + "/" + "recovery";

      testBase = new JMSTestBase();
      try {
        testBase.beforTest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
        testBase.afterTest();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.BlockReader.prop.scheme","jms");
    conf.set("dt.application.Ingestion.operator.MessageReader.prop.idempotentStorageManager.recoveryPath",testMeta.recoveryDir);

    conf.set("dt.application.Ingestion.operator.MessageReader.prop.connectionFactoryProperties.brokerURL", TestMeta.BROKER_URL);
    conf.set("dt.application.Ingestion.operator.MessageReader.prop.ackMode", "AUTO_ACKNOWLEDGE");
    conf.set("dt.application.Ingestion.operator.MessageReader.prop.subject", TestMeta.SUBJECT);

    conf.set("dt.application.Ingestion.operator.FileWriter.prop.filePath", testMeta.outputDirectory);

    conf.set("dt.output.protocol", "file");


    lma.prepareDAG(new Application(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    // Produce Messages
    JMSMessageProducer jmsMessageProducer = new JMSMessageProducer(TestMeta.BROKER_URL, TestMeta.SUBJECT);
    jmsMessageProducer.produceMsg(5);

    // Run application
    lc.runAsync();

    long now = System.currentTimeMillis();

    Path outDir = new Path(testMeta.outputDirectory);
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 60000) {
      Thread.sleep(100);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(100);
    lc.shutdown();
    
    Assert.assertTrue("output dir does not exist", fs.exists(outDir));
    File outputFile = new File(testMeta.outputDirectory).listFiles()[0];
    List<String> actual = FileUtils.readLines(outputFile);

    Assert.assertEquals("JMS tuple count not matching", 5, actual.size());
    Assert.assertEquals("JMS TextMessage not matching", "Test TextMessage : 0", actual.get(0));
    Assert.assertEquals("JMS StreamMessage not matching", "Test StreamMessage : 1",actual.get(1));
    Assert.assertEquals("JMS BytesMessage not matching", "Test BytesMessage : 2", actual.get(2));
    Assert.assertEquals("JMS MapMessage not matching", "{Msg:Test MapMessage : 3}", actual.get(3));

    Thread.sleep(1000);
    
    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();

  }

  private static final Logger LOG = LoggerFactory.getLogger(JMSApplicationTest.class);
}
