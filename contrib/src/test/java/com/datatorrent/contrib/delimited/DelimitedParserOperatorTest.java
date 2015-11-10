/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import java.io.File;
import java.io.IOException;

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

public class DelimitedParserOperatorTest
{

  private static final String filename = "schema.json";
  CollectorTestSink<Object> valid = new CollectorTestSink<Object>();
  CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  CollectorTestSink<Object> objectPort = new CollectorTestSink<Object>();
  DelimitedParserOperator parser = new DelimitedParserOperator();

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
    parser.error.setSink(error);
    parser.output.setSink(objectPort);
    parser.validatedData.setSink(valid);
    parser.setSchemaPath(testMeta.dirName + "/" + filename);
    parser.setup(null);
  }

  @After
  public void tearDown()
  {
    valid.clear();
    error.clear();
    objectPort.clear();
  }

  @Test
  public void TestParserValidInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(1, valid.collectedTuples.size());
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidAdIdInput()
  {
    String input = ",98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserNoCampaignIdInput()
  {
    String input = "1234,,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(1, valid.collectedTuples.size());
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidCampaignIdInput()
  {
    String input = "1234,9833,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidAdNameInput()
  {
    String input = "1234,98233,adxyz123,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidBidPriceInput()
  {
    String input = "1234,98233,adxyz,3.3,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidStartDateInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-30-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidSecurityCodeInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,85,y,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidisActiveInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,yo,,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidParentCampaignInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(1, valid.collectedTuples.size());
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP,Y,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInValidWeatherTargeting()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,NO,yes";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserNullOrBlankInput()
  {
    parser.input.process(null);
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserHeaderAsInput()
  {
    String input = "adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,active,optimized,parentCampaign,weatherTargeted,valid";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserLessFields()
  {
    parser.input.process("1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZATION".getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserMoreFields()
  {
    parser.input
        .process("1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes,ExtraField"
            .getBytes());
    parser.teardown();
    Assert.assertEquals(0, valid.collectedTuples.size());
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  private static final Logger logger = LoggerFactory.getLogger(DelimitedParserOperatorTest.class);
}
