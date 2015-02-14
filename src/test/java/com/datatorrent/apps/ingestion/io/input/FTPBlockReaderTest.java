/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.ingestion.io.input;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.Description;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link FTPBlockReader}.
 */
public class FTPBlockReaderTest extends BlockReaderTest
{
  public FTPBlockReaderTest()
  {
    this.testMeta = new FTPTestMeta();
  }

  @Override
  protected FSSliceReader getBlockReader()
  {
    FTPBlockReader reader = new FTPBlockReader();
    return reader;
  }

  public class FTPTestMeta extends TestMeta
  {
    FakeFtpServer fakeFtpServer;
    String ftpDir;
    String ftpUri;
    static final String SAMPLE_TEXT = "abcdefghjklmnopqrstuvwxyz";
    
    /* (non-Javadoc)
     * @see com.datatorrent.lib.io.block.FSSliceReaderTest.TestMeta#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      output = "target/" + description.getClassName() + "/" + description.getMethodName();
      try {
        FileUtils.forceMkdir(new File(output));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      ftpDir = new File(this.output + "/ftp").getAbsolutePath();
      
      fakeFtpServer = new FakeFtpServer();
      fakeFtpServer.setServerControlPort(9921);
      fakeFtpServer.addUserAccount(new UserAccount("testUser", "test", ftpDir));

      UnixFakeFileSystem fileSystem = new UnixFakeFileSystem();
      fileSystem.add(new FileEntry(ftpDir + "/abcd.txt", SAMPLE_TEXT));

      fakeFtpServer.setFileSystem(fileSystem);
      fakeFtpServer.start();

      ftpUri = "ftp://testUser:test@localhost:"+fakeFtpServer.getServerControlPort()+"/"+ftpDir+"/abcd.txt";
      blockReader = getBlockReader();
      ((FTPBlockReader)blockReader).setDirectory(ftpUri);
      
      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, Long.toHexString(System.currentTimeMillis()));
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = new OperatorContextTestHelper.TestIdOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<Object>();
      blockReader.messages.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);
      
    }

    @Override
    protected void finished(Description description)
    {
        super.finished(description);
        fakeFtpServer.stop();
    }
  }

  @Test
  public void testBytesReceived() throws IOException
  {
    long blockSize = 3;
    long fileLength = FTPTestMeta.SAMPLE_TEXT.length();
    int noOfBlocks = (int) ((fileLength / blockSize) + (((fileLength % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata = 
          new BlockMetadata.FileBlockMetadata(
              ((FTPTestMeta) testMeta).ftpUri, 
                i, i * blockSize, 
                i == noOfBlocks - 1 ? fileLength : (i + 1) * blockSize, 
                i == noOfBlocks - 1, i - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;

    StringBuffer outputSb = new StringBuffer();
    for (Object message : messages) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<Slice> msg = (AbstractBlockReader.ReaderRecord<Slice>) message;
      outputSb.append(new String(msg.getRecord().buffer));
    }

    Assert.assertEquals("Output not matching", outputSb.toString(), FTPTestMeta.SAMPLE_TEXT);
  }

  @Override
  public void testFailedBlocks()
  {
    // suppress this test

  }

  @Override
  public void testIdleTimeHandlingOfFailedBlocks()
  {
    // suppress this test
  }

  @Override
  public void testNumRetries()
  {
    // suppress this test
  }
}