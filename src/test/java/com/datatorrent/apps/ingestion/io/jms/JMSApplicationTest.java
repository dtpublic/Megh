/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import java.io.File;
import java.io.IOException;

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
import com.datatorrent.apps.ingestion.JMSMessageApplication;
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
        FileUtils.deleteDirectory(new File(baseDirectory));
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
    conf.set("dt.application.JMSMessageIngestionApp.operator.MessageReader.prop.idempotentStorageManager.recoveryPath",testMeta.recoveryDir);

    conf.set("dt.application.JMSMessageIngestionApp.operator.MessageReader.prop.connectionFactoryProperties.brokerURL", TestMeta.BROKER_URL);
    conf.set("dt.application.JMSMessageIngestionApp.operator.MessageReader.prop.ackMode", "AUTO_ACKNOWLEDGE");
    conf.set("dt.application.JMSMessageIngestionApp.operator.MessageReader.prop.subject", TestMeta.SUBJECT);

    conf.set("dt.application.JMSMessageIngestionApp.operator.FileWriter.prop.filePath", testMeta.outputDirectory);

    lma.prepareDAG(new JMSMessageApplication(), conf);
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
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));
    File outputFile = new File(testMeta.outputDirectory).listFiles()[0];
    String actual = FileUtils.readFileToString(outputFile);
    String expected = "Test Message : 0\nTest Message : 1\nTest Message : 2\nTest Message : 3\nTest Message : 4\n";
    Assert.assertEquals("JMS output not matching", expected, actual);
    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();

  }

  private static final Logger LOG = LoggerFactory.getLogger(JMSApplicationTest.class);
}
