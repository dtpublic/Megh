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
import com.datatorrent.apps.ingestion.io.output.OutputFileMerger;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.FilePartitionBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.StaticStringBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.google.common.collect.Lists;


/**
 * 
 */
public class PartitionWriterTest
{
  
  public static final int BLOCK_SIZE = 10;
  

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<FileMetadata> fileMetadataList = Lists.newArrayList();
    
    OutputFileMerger<PatitionMetaData> oper;
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
      
      oper = new OutputFileMerger<PatitionMetaData>();
      oper.setFilePath(outputPath);
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
        
        long blockID=1000;
        
        for (int i = 0; i < PartitionMetaDataEmitterTest.FILE_CONTENTS.length; i++) {
          List<Long> blockIDs = Lists.newArrayList();
          
          File file = new File(outputPath, i + ".txt");

          FileUtils.write(file, PartitionMetaDataEmitterTest.FILE_CONTENTS[i]);

          
          int offset=0;
          for(; offset< PartitionMetaDataEmitterTest.FILE_CONTENTS[i].length(); offset+= BLOCK_SIZE, blockID++){
            String blockContents;
            if(offset+BLOCK_SIZE < PartitionMetaDataEmitterTest.FILE_CONTENTS[i].length()){
              blockContents= PartitionMetaDataEmitterTest.FILE_CONTENTS[i].substring(offset, offset+BLOCK_SIZE);
            }
            else{
              blockContents= PartitionMetaDataEmitterTest.FILE_CONTENTS[i].substring(offset);
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
  
  
  public void testPartitionWriting(String seperator,long[][][] partitionMeta, String [] expectedOutput) throws IOException, InterruptedException{
    testMeta.oper.beginWindow(0);
    long partitionID = 0;
    for(int tupleIndex=0; tupleIndex<partitionMeta.length; tupleIndex++){
      String partitionFileName = "testArchive_"+partitionID+".partition" ;
      List<PartitionBlockMetaData> fileBlockMetadataList = Lists.newArrayList();
      
      for(long[] block: partitionMeta[tupleIndex]){
        
        if(block[0] == -1){
          //Add separator block
          StaticStringBlockMetaData staticStringBlockMetaData = new StaticStringBlockMetaData(seperator,block[1],block[2]);
          fileBlockMetadataList.add(staticStringBlockMetaData);
        }
        else{
          //Add file block
          FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(new File(testMeta.blocksDir, block[0]+"").getPath(), 
              block[0], block[1], block[2], false, -1);
          
          FilePartitionBlockMetaData filePartitionBlockMetaData = 
              new FilePartitionBlockMetaData(fileBlockMetadata, null,false);
          fileBlockMetadataList.add(filePartitionBlockMetaData);
        }
      }
      
      PatitionMetaData patitionMetaData = new PatitionMetaData(partitionID++, partitionFileName, fileBlockMetadataList);
      testMeta.oper.input.process(patitionMetaData);
    }
    testMeta.oper.endWindow();
    testMeta.oper.committed(0);
    //give some time to complete postCommit operations
    Thread.sleep(2*1000);
    
    
    for(int partID=0; partID < partitionID; partID++ ){
      String fromFile = FileUtils.readFileToString(new File(testMeta.oper.getFilePath(), "testArchive_"+partID+".partition"));
      Assert.assertEquals("Partition "+ partID+"not matching", expectedOutput[partID], fromFile);
    }
  }
  
  @Test
  public void testSingleCharacterSeperator() throws IOException, InterruptedException{
    final String fileSeperator = "#";
    String [] expected ={
        "abcde#pq", "r#xyz#AB", "CDEFGHIJ", "KLMNOPQR", "STUVWXYZ", "#0123456", "789#"
    };
    testPartitionWriting(fileSeperator, PartitionMetaDataEmitterTest.BLOCKS_META_SINGLE_CHAR,expected);
  }
  
  @Test
  public void testMultiCharacterSeperator() throws IOException, InterruptedException{
    final String fileSeperator = "===END_OF_FILE===";
    String [] expected ={
        "abcde===",
        "END_OF_F",
        "ILE===pq",
        "r===END_",
        "OF_FILE=",
        "==xyz===",
        "END_OF_F",
        "ILE===AB",
        "CDEFGHIJ",
        "KLMNOPQR",
        "STUVWXYZ",
        "===END_O",
        "F_FILE==",
        "=0123456",
        "789===EN",
        "D_OF_FIL",
        "E==="
    };
    testPartitionWriting(fileSeperator, PartitionMetaDataEmitterTest.BLOCKS_META_MULTI_CHAR,expected);
  }
  
}
