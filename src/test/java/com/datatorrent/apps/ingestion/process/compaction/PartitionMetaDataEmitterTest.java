/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.FilePartitionBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.StaticStringBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.collect.Lists;

/**
 * 
 */
public class PartitionMetaDataEmitterTest
{
  public static final String[] FILE_CONTENTS = {
    "abcde", "pqr", "xyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789"};

  public static final int BLOCK_SIZE = 10;

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<IngestionFileMetaData> fileMetadataList = Lists.newArrayList();
    
    PartitionMetaDataEmitter oper;
    Context.OperatorContext context;
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      
      oper = new PartitionMetaDataEmitter();
      oper.compactionBundleName = "testArchive";
      oper.partitionSizeInBytes = 8;
      String appDirectory = outputPath;
      
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "PartitionMetaDataEmitterTest");
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      oper.setup(context);
      
      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);
        
        File blocksDir = new File(context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS);
        FileUtils.forceMkdir(blocksDir);
        
        long blockID=1000;
        
        for (int i = 0; i < FILE_CONTENTS.length; i++) {
          List<Long> blockIDs = Lists.newArrayList();
          
          File file = new File(outputPath, i + ".txt");

          FileUtils.write(file, FILE_CONTENTS[i]);

          
          int offset=0;
          for(; offset< FILE_CONTENTS[i].length(); offset+= BLOCK_SIZE, blockID++){
            String blockContents;
            if(offset+BLOCK_SIZE < FILE_CONTENTS[i].length()){
              blockContents= FILE_CONTENTS[i].substring(offset, offset+BLOCK_SIZE);
            }
            else{
              blockContents= FILE_CONTENTS[i].substring(offset);
            }
            FileUtils.write(new File(blocksDir, blockID+""), blockContents);
            blockIDs.add(blockID);
          }
          
          IngestionFileMetaData fileMetadata = new IngestionFileMetaData(file.getPath());
          fileMetadata.setBlockIds(ArrayUtils.toPrimitive(blockIDs.toArray(new Long[0])));
          System.out.println(file+":"+blockIDs);
          fileMetadataList.add(fileMetadata);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#finished(org.junit.runner.Description)
     */
    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      
      try {
        FileUtils.deleteDirectory(new File(outputPath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Rule
  public TestMeta testMeta = new TestMeta();
  
  
  private void testPartitionMetaData(long [][][] expected, String seperator){
    
    CollectorTestSink<PatitionMetaData> sink = new CollectorTestSink<PatitionMetaData>();
    testMeta.oper.patitionMetaDataOutputPort.setSink((CollectorTestSink) sink);
    testMeta.oper.setFileBoundarySeperator(seperator);
    

    for (FileMetadata fileMetadata : testMeta.fileMetadataList) {
      testMeta.oper.processedFileInput.process(fileMetadata);
    }
    testMeta.oper.teardown();
    
    
    
    int tupleIndex=0;
    for(PatitionMetaData patitionMetaData: sink.collectedTuples){
      List<PartitionBlockMetaData> blockMetaDataList = patitionMetaData.getBlockMetaDataList();
      int blockMetaDataIndex=0;

      for(PartitionBlockMetaData blockMetaData: blockMetaDataList){
        long [] ans= expected[tupleIndex][blockMetaDataIndex];
        if(blockMetaData instanceof FilePartitionBlockMetaData){
          Assert.assertEquals("BlockId is not matching",ans[0], blockMetaData.getBlockId());
          Assert.assertEquals("Offset is not matching",ans[1], blockMetaData.getOffset());
          Assert.assertEquals("Length is not matching",ans[2], blockMetaData.getLength());
          blockMetaDataIndex++;
        }
        else if(blockMetaData instanceof StaticStringBlockMetaData){
          StaticStringBlockMetaData staticStringBlockMetaData = (StaticStringBlockMetaData) blockMetaData;
          Assert.assertEquals("BlockId is not matching",ans[0], staticStringBlockMetaData.getBlockId());
          Assert.assertEquals("Offset is not matching",ans[1], staticStringBlockMetaData.getOffset());
          Assert.assertEquals("Length is not matching",ans[2], staticStringBlockMetaData.getLength());
          blockMetaDataIndex++;
        }
      }
      tupleIndex++;
    }
  }

  public static final long[][][] BLOCKS_META_SINGLE_CHAR = {
    {{1000,0,5},{-1,0,1},{1001,0,2}}, 
    {{1001,2,1},{-1,0,1},{1002,0,3},{-1,0,1},{1003,0,2}},
    {{1003,2,8}},
    {{1004,0,8}},
    {{1004,8,2},{1005,0,6}},
    {{-1,0,1},{1006,0,7}},
    {{1006,7,3},{-1,0,1}}
  };

  @Test
  public void testSingleCharacterSeperator(){
    final String fileSeperator = "#";
    testPartitionMetaData(BLOCKS_META_SINGLE_CHAR, fileSeperator);
  }
  
  public static final long[][][] BLOCKS_META_MULTI_CHAR = {
    {{1000,0,5},{-1,0,3}},
    {{-1,3,8}},
    {{-1,11,6},{1001,0,2}},
    {{1001,2,1},{-1,0,7}},
    {{-1,7,8}},
    {{-1,15,2},{1002,0,3},{-1,0,3}},
    {{-1,3,8}},
    {{-1,11,6},{1003,0,2}},
    {{1003,2,8}},
    {{1004,0,8}},
    {{1004,8,2},{1005,0,6}},
    {{-1,0,8}},
    {{-1,8,8}},
    {{-1,16,1},{1006,0,7}},
    {{1006,7,3},{-1,0,5}},
    {{-1,5,8}},
    {{-1,13,4}}
  };
  
  @Test
  public void testMultiCharacterSeperator(){
    final String fileSeperator = "===END_OF_FILE===";
    testPartitionMetaData(BLOCKS_META_MULTI_CHAR, fileSeperator);
  }
  
  
}
