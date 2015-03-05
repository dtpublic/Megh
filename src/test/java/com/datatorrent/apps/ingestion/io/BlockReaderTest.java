/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.FSSliceReaderTest;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class BlockReaderTest extends FSSliceReaderTest
{

  @Override
  protected FSSliceReader getBlockReader()
  {
    BlockReaderWhichFails reader = new BlockReaderWhichFails();
    reader.scheme = "file";
    reader.setMaxRetries(5);
    return reader;
  }

  @Override
  public void testBytesReceived() throws IOException
  {
    //suppressing test
  }

  @Test
  public void testFailedBlocks()
  {
    long blockSize = 1500;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));
    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(testMeta.dataFile.getAbsolutePath(), i, i * blockSize,
        i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize,
        i == noOfBlocks - 1, i - 1);

      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    Assert.assertEquals("num failed blocks", noOfBlocks, ((BlockReader) testMeta.blockReader).failedQueue.size());
  }

  @Test
  public void testNumRetries()
  {
    CollectorTestSink<BlockMetadata.FileBlockMetadata> failedBlocks = new CollectorTestSink<BlockMetadata.FileBlockMetadata>();
    TestUtils.setSink(((BlockReader) testMeta.blockReader).error, failedBlocks);

    long blockSize = 1500;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testFailedBlocks();

    for (int i = 2; i < 7; i++) {
      testMeta.blockReader.beginWindow(i);
      int count = 0;
      while (count++ < noOfBlocks) {
        testMeta.blockReader.handleIdleTime();
      }

      testMeta.blockReader.endWindow();
    }

    Assert.assertEquals("max retries", noOfBlocks, failedBlocks.collectedTuples.size());
  }

  @Test
  public void testIdleTimeHandlingOfFailedBlocks()
  {
    CollectorTestSink<BlockMetadata.FileBlockMetadata> failedBlocks = new CollectorTestSink<BlockMetadata.FileBlockMetadata>();
    TestUtils.setSink(((BlockReader) testMeta.blockReader).error, failedBlocks);

    long blockSize = 1500;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));
    testFailedBlocks();

    testMeta.blockReader.beginWindow(2);

    int count = 0;
    while (count++ < (noOfBlocks * 5)) {
      testMeta.blockReader.handleIdleTime();
    }

    testMeta.blockReader.endWindow();
    Assert.assertEquals("process failed blocks in idle time", noOfBlocks, failedBlocks.collectedTuples.size());
  }

  private static class BlockReaderWhichFails extends BlockReader
  {
    @Override
    protected void readBlock(BlockMetadata blockMetadata) throws IOException
    {
      throw new IOException();
    }

  }

}