/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileCreator.IndexEntry;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.FileInfoBlockMetadata;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.FilePartitionInfo;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * 
 */
public class MetaFileCreatorTest
{
  private class TestMeta extends TestWatcher
  {
    MetaFileCreator oper;
    Context.OperatorContext context;

    /*
     * (non-Javadoc)
     * 
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      // TODO Auto-generated method stub
      super.starting(description);
      oper = new MetaFileCreator();

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "MetaFileCreatorTest");
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      oper.setup(context);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.junit.rules.TestWatcher#finished(org.junit.runner.Description)
     */
    @Override
    protected void finished(Description description)
    {
      // TODO Auto-generated method stub
      super.finished(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testPartitionSynchronization()
  {

    FilePartitionInfo[] filePartitions = populateFilePartitionInfo();
    PatitionMetaData[] partitionMetaDatas = populatePartitionMetaData();

    for (FilePartitionInfo filePartition : filePartitions) {
      testMeta.oper.filePartitionInfoPort.process(filePartition);
    }

    CollectorTestSink<IndexEntry> sink = new CollectorTestSink<IndexEntry>();
    testMeta.oper.indexEntryOuputPort.setSink((CollectorTestSink) sink);
    Assert.assertEquals("[]", sink.collectedTuples.toString());

    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[2]);
    Assert.assertEquals("[]", sink.collectedTuples.toString());

    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[1]);
    Assert.assertEquals("[]", sink.collectedTuples.toString());

    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[0]);
    Assert.assertEquals("[0\t0\t0\t25\tfile0\n, 0\t25\t0\t90\tfile1\n, 0\t90\t1\t50\tfile2\n]", sink.collectedTuples.toString());

    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[3]);
    Assert.assertEquals("[0\t0\t0\t25\tfile0\n, 0\t25\t0\t90\tfile1\n, 0\t90\t1\t50\tfile2\n]", sink.collectedTuples.toString());

    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[4]);
    Assert.assertEquals("[0\t0\t0\t25\tfile0\n, 0\t25\t0\t90\tfile1\n" 
        + ", 0\t90\t1\t50\tfile2\n, 1\t50\t4\t10\tfile3\n, 4\t10\t4\t50\tfile4\n," 
        + " 4\t50\t4\t80\tfile5\n, 4\t80\t4\t100\tfile6\n]", sink.collectedTuples.toString());

  }

  /**
   * @return
   */
  private FilePartitionInfo[] populateFilePartitionInfo()
  {
    long[][] partitionInfo = {
        // {startPartitionId, startOffset, endPartitionId, endOffset}
        { 0, 0, 0, 25 }, { 0, 25, 0, 90 }, { 0, 90, 1, 50 }, { 1, 50, 4, 10 }, { 4, 10, 4, 50 }, { 4, 50, 4, 80 }, { 4, 80, 4, 100 } };
    FilePartitionInfo[] filePartitions = new FilePartitionInfo[partitionInfo.length];
    for (int i = 0; i < partitionInfo.length; i++) {
      long[] fileEntry = partitionInfo[i];
      IngestionFileMetaData ingestionFileMetaData = new IngestionFileMetaData();
      ingestionFileMetaData.setRelativePath("file" + i);
      filePartitions[i] = new FilePartitionInfo(ingestionFileMetaData, fileEntry[0], fileEntry[1]);
      filePartitions[i].setEndPartitionId(fileEntry[2]);
      filePartitions[i].setEndOffset(fileEntry[3]);
    }
    return filePartitions;
  }

  /**
   * @return
   */
  private PatitionMetaData[] populatePartitionMetaData()
  {
    long[][] partitionToFile = { { 0, 1, 2 }, { 2, 3 }, { 3 }, { 3 }, { 3, 4, 5, 6 } };

    PatitionMetaData[] patitionMetaData = new PatitionMetaData[5];
    for (int i = 0; i < partitionToFile.length; i++) {
      List<FileInfoBlockMetadata> fileBlockMetadatas = new ArrayList<FileInfoBlockMetadata>();
      for (long fileId : partitionToFile[i]) {
        fileBlockMetadatas.add(new FileInfoBlockMetadata("file" + fileId, true, null, 0, 0, 0, false, -1));
      }
      patitionMetaData[i] = new PatitionMetaData(i, null, fileBlockMetadatas);
    }

    return patitionMetaData;
  }

}
