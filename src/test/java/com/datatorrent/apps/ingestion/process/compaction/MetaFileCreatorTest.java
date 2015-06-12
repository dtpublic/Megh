/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileCreator.IndexEntry;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.FilePartitionBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.FilePartitionInfo;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;

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
    Assert.assertEquals(3, sink.collectedTuples.size());
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",0,0,0,25,0), sink.collectedTuples.get(0));
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",0,25,0,90,1), sink.collectedTuples.get(1));
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",0,90,1,50,2), sink.collectedTuples.get(2));
    
    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[3]);
    Assert.assertEquals(3, sink.collectedTuples.size());
    
    testMeta.oper.partitionCompleteTrigger.process(partitionMetaDatas[4]);
    Assert.assertEquals(7, sink.collectedTuples.size());
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",1,50,4,10,3), sink.collectedTuples.get(3));
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",4,10,4,50,4), sink.collectedTuples.get(4));
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",4,50,4,80,5), sink.collectedTuples.get(5));
    Assert.assertEquals(String.format("-  %16d%16d%16d%16d file%d\n",4,80,4,100,6), sink.collectedTuples.get(6));

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
      List<PartitionBlockMetaData> fileBlockMetadatas = new ArrayList<PartitionBlockMetaData>();
      for (long fileId : partitionToFile[i]) {
        FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(null, 0, 0, 0, false, -1);
        fileBlockMetadatas.add(new FilePartitionBlockMetaData(fileBlockMetadata, "file"+fileId, true));
      }
      patitionMetaData[i] = new PatitionMetaData(i, null, fileBlockMetadatas);
    }

    return patitionMetaData;
  }

}
