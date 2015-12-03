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
package com.datatorrent.apps.ingestion.io.ftp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockftpserver.core.command.Command;
import org.mockftpserver.core.command.CommandNames;
import org.mockftpserver.core.command.ReplyCodes;
import org.mockftpserver.core.session.Session;
import org.mockftpserver.core.util.StringUtil;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.command.AbstractFakeCommandHandler;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystemEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.malhar.lib.io.block.AbstractBlockReader;
import com.datatorrent.malhar.lib.io.block.BlockMetadata;
import com.datatorrent.netlet.util.Slice;

/**
 * Tests for {@link FTPBlockReader}.
 */
public class FTPBlockReaderTest
{
  public class TestMeta extends TestWatcher
  {
    FakeFtpServer fakeFtpServer;
    String ftpDir;
    String filePath;
    String output;
    FTPBlockReader blockReader;
    CollectorTestSink<Object> messageSink;
    CollectorTestSink<Object> blockMetadataSink;
    Context.OperatorContext readerContext;

    static final String SAMPLE_TEXT = "abcdefghjklmnopqrstuvwxyz";

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
      filePath = ftpDir + "/abcd.txt";

      fakeFtpServer = new FakeFtpServer();
      fakeFtpServer.setServerControlPort(9921);
      fakeFtpServer.addUserAccount(new UserAccount("testUser", "test", ftpDir));
      fakeFtpServer.setCommandHandler(CommandNames.LIST, new DTListCommandHandler());

      UnixFakeFileSystem fileSystem = new UnixFakeFileSystem();
      fileSystem.add(new FileEntry(ftpDir + "/abcd.txt", SAMPLE_TEXT));

      fakeFtpServer.setFileSystem(fileSystem);
      fakeFtpServer.start();

      blockReader = new FTPBlockReader();

      blockReader.setHost("localhost");
      blockReader.setPort(fakeFtpServer.getServerControlPort());
      blockReader.setUserName("testUser");
      blockReader.setPassword("test");

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(Context.DAGContext.APPLICATION_ID, Long.toHexString(System.currentTimeMillis()));
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
      blockReader.teardown();
      fakeFtpServer.stop();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testBytesReceived() //throws IOException
  {
    long blockSize = 3;
    long fileLength = TestMeta.SAMPLE_TEXT.length();
    int noOfBlocks = (int) ((fileLength / blockSize) + (((fileLength % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata =
        new BlockMetadata.FileBlockMetadata(testMeta.filePath,
          i, i * blockSize,
          i == noOfBlocks - 1 ? fileLength : (i + 1) * blockSize,
          i == noOfBlocks - 1, i - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;

    StringBuilder outputSb = new StringBuilder();
    for (Object message : messages) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<Slice> msg = (AbstractBlockReader.ReaderRecord<Slice>) message;
      outputSb.append(new String(msg.getRecord().buffer));
    }

    Assert.assertEquals("Output not matching", outputSb.toString(), TestMeta.SAMPLE_TEXT);
  }

  @Test
  public void testFtpUri() //throws IOException
  {
    long blockSize = 3;
    long fileLength = TestMeta.SAMPLE_TEXT.length();
    int noOfBlocks = (int) ((fileLength / blockSize) + (((fileLength % blockSize) == 0) ? 0 : 1));
    String uri = "ftp://testUser:test@localhost:" + testMeta.fakeFtpServer.getServerControlPort() + "/" + testMeta.ftpDir + "/abcd.txt";
    testMeta.blockReader.setUri(uri);
    testMeta.blockReader.setup(testMeta.readerContext);

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata =
        new BlockMetadata.FileBlockMetadata(testMeta.filePath,
          i, i * blockSize,
          i == noOfBlocks - 1 ? fileLength : (i + 1) * blockSize,
          i == noOfBlocks - 1, i - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;

    StringBuilder outputSb = new StringBuilder();
    for (Object message : messages) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<Slice> msg = (AbstractBlockReader.ReaderRecord<Slice>) message;
      outputSb.append(new String(msg.getRecord().buffer));
    }
    Assert.assertEquals("Output not matching", outputSb.toString(), TestMeta.SAMPLE_TEXT);
  }
  
  class DTListCommandHandler extends AbstractFakeCommandHandler{

    @Override
    protected void handle(Command command, Session session)
    {
      verifyLoggedIn(session);
      sendReply(session, ReplyCodes.TRANSFER_DATA_INITIAL_OK);

      String path = getRealPath(session, command.getParameter(0));
      if( !getFileSystem().exists(path)){
         path = getRealPath(session, command.getParameter(1));
      }
      // User must have read permission to the path
      if (getFileSystem().exists(path)) {
        this.replyCodeForFileSystemException = ReplyCodes.READ_FILE_ERROR;
        verifyReadPermission(session, path);
      }

      this.replyCodeForFileSystemException = ReplyCodes.SYSTEM_ERROR;
      List fileEntries = getFileSystem().listFiles(path);
      Iterator iter = fileEntries.iterator();
      List lines = new ArrayList();
      while (iter.hasNext()) {
        FileSystemEntry entry = (FileSystemEntry) iter.next();
        lines.add(getFileSystem().formatDirectoryListing(entry));
      }
      String result = StringUtil.join(lines, endOfLine());
      result += result.length() > 0 ? endOfLine() : "";

      session.openDataConnection();
      LOG.info("Sending [" + result + "]");
      session.sendData(result.getBytes(), result.length());
      session.closeDataConnection();

      sendReply(session, ReplyCodes.TRANSFER_DATA_FINAL_OK);
    }
  }
}
