/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.netlet.util.DTThrowable;

public class DelimitedToPojoConverterTest
{

  private static final String filename = "schema.json";
  CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  CollectorTestSink<Object> outputPort = new CollectorTestSink<Object>();
  DelimitedToPojoConverterOperator converter = new DelimitedToPojoConverterOperator();

  @ClassRule
  public static TestMeta testMeta = new TestMeta();

  public static class TestMeta extends TestWatcher
  {
    public org.junit.runner.Description desc;
    public String dirName;

    @Override
    protected void starting(Description description)
    {
      this.desc = description;
      super.starting(description);
      dirName = "target/" + desc.getClassName() + "/" + desc.getMethodName();
      new File(dirName).mkdir();
      FileSystem hdfs = null;
      //Creating a file in HDFS
      Path newFilePath = new Path(dirName + "/" + filename);
      try {
        hdfs = FileSystem.get(new Configuration());
        hdfs.createNewFile(newFilePath);
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
      //Writing data to a HDFS file
      StringBuilder sb = new StringBuilder();
      sb.append(SchemaUtils.jarResourceFileToString(filename));
      byte[] byt = sb.toString().getBytes();
      try {
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
        fsOutStream.write(byt);
        fsOutStream.close();
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);

      }
      logger.debug("Written data to HDFS file.");

    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(dirName));
    }

  }

  /*
   * adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,isActive,isOptimized,parentCampaign,weatherTargeted,valid
   * 1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes
   * Constraints are defined in schema.json
   */

  @Before
  public void setup()
  {
    converter.error.setSink(error);
    converter.output.setSink(outputPort);
    converter.setClazz(Ad.class);
    converter.setSchemaPath(testMeta.dirName + "/" + filename);
    converter.setup(null);
  }

  @After
  public void tearDown()
  {
    error.clear();
    outputPort.clear();
  }

  @Test
  public void TestParserValidInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    converter.input.process(input.getBytes());
    converter.teardown();
    Assert.assertEquals(1, outputPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = outputPort.collectedTuples.get(0);
    Ad adPojo = (Ad)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals(Ad.class, obj.getClass());
    Assert.assertEquals(1234, adPojo.getAdId());
    Assert.assertTrue("adxyz".equals(adPojo.getAdName()));
    Assert.assertEquals(0.2, adPojo.getBidPrice(), 0.0);
    Assert.assertEquals(Date.class, adPojo.getStartDate().getClass());
    Assert.assertEquals(Date.class, adPojo.getEndDate().getClass());
    Assert.assertEquals(12, adPojo.getSecurityCode());
    Assert.assertTrue("CAMP_AD".equals(adPojo.getParentCampaign()));
    Assert.assertTrue(adPojo.isActive());
    Assert.assertFalse(adPojo.isOptimized());
  }

  @Test
  public void TestParserInvalidisActiveInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,yo,OPTIMIZE,CAMP_AD,Y,yes";
    converter.input.process(input.getBytes());
    converter.teardown();
    Assert.assertEquals(0, outputPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    byte[] obj = (byte[])error.collectedTuples.get(0);
    Assert.assertTrue(new String(obj).equals(input));
  }

  @Test
  public void TestParserValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes";
    converter.input.process(input.getBytes());
    converter.teardown();
    Assert.assertEquals(1, outputPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = outputPort.collectedTuples.get(0);
    Ad adPojo = (Ad)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals(Ad.class, obj.getClass());
    Assert.assertTrue(adPojo.isOptimized());
  }

  @Test
  public void TestParserInValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes";
    converter.input.process(input.getBytes());
    converter.teardown();
    Assert.assertEquals(0, outputPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    byte[] obj = (byte[])error.collectedTuples.get(0);
    Assert.assertTrue(new String(obj).equals(input));
  }

  @Test
  public void TestParserNullOrBlankInput()
  {
    converter.input.process(null);
    converter.teardown();
    Assert.assertEquals(0, outputPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    byte[] obj = (byte[])error.collectedTuples.get(0);
    Assert.assertNull(obj);
  }

  @Test
  public void TestParserHeaderAsInput()
  {
    String header = "adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,active,optimized,parentCampaign,weatherTargeted,valid";
    converter.input.process(header.getBytes());
    converter.teardown();
    Assert.assertEquals(0, outputPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    byte[] obj = (byte[])error.collectedTuples.get(0);
    Assert.assertTrue(new String(obj).equals(header));
  }

  private static final Logger logger = LoggerFactory.getLogger(DelimitedToPojoConverterTest.class);

}
