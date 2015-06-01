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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.FileInfoBlockMetadata;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.collect.Lists;


/**
 * 
 */
public class PartitionWriterTest
{
  public static final String[] FILE_CONTENTS = {
    "abcde", "pqr", "xyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789"};
  
  public static final String FILE_SEPERATION_CHAR = "";
  public static final int BLOCK_SIZE = 10;
  

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<FileMetadata> fileMetadataList = Lists.newArrayList();
    
    PartitionWriter oper;
    File blocksDir;
    Context.OperatorContext context;
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();
      
      oper = new PartitionWriter();
      oper.setOutputDir(outputPath);
      String appDirectory = outputPath;
      
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "PartitionWriterTest");
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      oper.setup(context);
      
      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);
        
        blocksDir = new File(context.getValue(Context.DAGContext.APPLICATION_PATH) , BlockWriter.SUBDIR_BLOCKS);
        blocksDir.mkdirs();
        //FileUtils.forceMkdir(blocksDir);
        
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
          
          FileMetadata fileMetadata = new FileMetadata(file.getPath());
          fileMetadata.setBlockIds(ArrayUtils.toPrimitive(blockIDs.toArray(new Long[0])));
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
  
  
  @Test
  public void testCompaction() throws IOException, InterruptedException{
    
    long[][][] partitionMeta= {
        {{1000,0,5},{1001,0,3}},
        {{1002,0,3},{1003,0,5}},
        {{1003,5,5},{1004,0,3}},
        {{1004,3,7},{1005,0,1}},
        {{1005,1,5},{1006,0,3}},
        {{1006,3,7}}
    };
    
    testMeta.oper.beginWindow(0);
    long partitionID = 0;
    for(int tupleIndex=0; tupleIndex<partitionMeta.length; tupleIndex++){
      String partitionFileName = "testArchive_"+partitionID+".partition" ;
      List<FileInfoBlockMetadata> fileBlockMetadataList = Lists.newArrayList();
      
      for(long[] block: partitionMeta[tupleIndex]){
        FileInfoBlockMetadata fileBlockMetadata = new FileInfoBlockMetadata(null,false,new File(testMeta.blocksDir, block[0]+"").getPath(), 
            block[0], block[1], block[2], false, -1);
        fileBlockMetadataList.add(fileBlockMetadata);
      }
      
      PatitionMetaData patitionMetaData = new PatitionMetaData(partitionID++, partitionFileName, fileBlockMetadataList);
      testMeta.oper.input.process(patitionMetaData);
    }
    testMeta.oper.endWindow();
    testMeta.oper.committed(0);
    //give some time to complete postCommit operations
    Thread.sleep(2*1000);
    String [] expected ={
        "abcdepqr", "xyzABCDE", "FGHIJKLM", "NOPQRSTU", "VWXYZ012", "3456789"
    };
    
    for(int partID=0; partID < partitionID; partID++ ){
      String fromFile = FileUtils.readFileToString(new File(testMeta.oper.getOutputDir(), "testArchive_"+partID+".partition"));
      Assert.assertEquals("Partition "+ partID+"not matching", expected[partID], fromFile);
    }
  }
  
  
  
}
