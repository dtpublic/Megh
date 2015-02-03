/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.DataInputStream;
import java.io.IOException;

import javax.management.RuntimeErrorException;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.io.fs.FileSplitter;

/**
 * This merges the various small block files to the main file
 * 
 * @author Yogi/Sandeep
 */

public class HdfsFileMerger extends BaseOperator
{
  protected transient FileSystem blocksFS, outputFS;
  @NotNull
  protected String filePath;
  protected String blocksPath;

  private boolean deleteSubFiles = true;
  protected boolean overwriteOutputFile = false;

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }

  private static final int BLOCK_SIZE = 1024 * 64; // 64 KB, default buffer size on HDFS
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileMerger.class);

  public HdfsFileMerger()
  {
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    blocksPath = context.getValue(DAG.APPLICATION_PATH) + "/blocks";
    try {
      outputFS = FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
      blocksFS = FileSystem.newInstance((new Path(blocksPath)).toUri(), new Configuration());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public final transient DefaultInputPort<FileSplitter.FileMetadata> processedFileInput = new DefaultInputPort<FileSplitter.FileMetadata>() {
    @Override
    public void process(FileSplitter.FileMetadata fileMetadata)
    {
      mergeFiles(fileMetadata);
    }
  };

  @Override
  public void teardown()
  {
    try {
      if (blocksFS != null) {
        blocksFS.close();
      }
      if (outputFS != null) {
        outputFS.close();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    blocksFS = null;
    outputFS = null;
  }

  private void mergeFiles(FileSplitter.FileMetadata fileMetadata)
  {
    
  String fileName = fileMetadata.getFileName();
  Path outputFilePath = new Path(filePath, fileName);
    try {
      if (outputFS.exists(outputFilePath)) {
        if (overwriteOutputFile) {
          outputFS.delete(outputFilePath, false);
        } else {
          LOG.info("Output file {} already exits and overwrite flag is off.", outputFilePath);
          LOG.info("Skipping writing output file.");
          return;
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to check existance of outputfile or delete it.");
      throw new RuntimeException("Exception during checking of existance of outputfile or deletion of the same.",e);
    }

    int numBlocks = fileMetadata.getNumberOfBlocks();

    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksPath, Long.toString(blocksArray[0]));// The first incomplete block.

    // Should we check if the last block is also = HDFS block size?
    Path lastBlock = new Path(blocksPath, Long.toString(blocksArray[numBlocks - 1]));// The last incomplete block.

    // File ==  1 block only.
    if(numBlocks == 1){
      moveFile(firstBlock,outputFilePath);
      return;
    }
    
  boolean dfsAppendSupport = Boolean.getBoolean(outputFS.getConf().get("dfs.support.append"));
  long defaultBlockSize = outputFS.getDefaultBlockSize(new Path(fileMetadata.getFilePath()));
  boolean sameSize = matchBlockSize(fileMetadata,defaultBlockSize);
  LOG.debug("Append flag is: {}",dfsAppendSupport);
  LOG.debug("defaultBlockSize flag/size is: {}",defaultBlockSize);
  LOG.debug("sameSize flag is: {}",sameSize);
  LOG.debug("Output scheme flag is: {}",outputFS.getScheme());

//  LOG.debug("instanceof flag is: {}",(outputFS instanceof DistributedFileSystem));
//    if(sameSize && dfsAppendSupport && outputFS instanceof DistributedFileSystem){
      if(sameSize && "hdfs".equalsIgnoreCase(outputFS.getScheme())){
      // Stitch and append the file. Extremely fast.
      // Conditions:
      // 1. dfs.support.append should be true
      // 2. block reader blocks size = HDFS block size
      // 3. Output should be on HDFS 
      LOG.info("Attempting fast merge.");
      try {
        stitchAndAppend(fileMetadata);
        return;
      } catch (Exception e) {
        LOG.error("Fast merge failed. {}",e);
        throw new RuntimeException("Unable to merge file on HDFS: " + fileMetadata.getFileName());
      }
    }
    mergeBlocksSerially(fileMetadata);
  }
  
  private void mergeBlocksSerially(FileSplitter.FileMetadata fileMetadata)
  {

    String fileName = fileMetadata.getFileName();
    LOG.debug(" filePath {}/{}", filePath, fileName);
    Path path = new Path(filePath, fileName);
    Path[] blockFiles = new Path[fileMetadata.getNumberOfBlocks()];
    int index = 0;
    for (long blockId : fileMetadata.getBlockIds()) {
      blockFiles[index] = new Path(blocksPath, Long.toString(blockId));
      index++;
    }
    FSDataOutputStream outputStream = null;
    try {
//      if (outputFS.exists(path)) {
//        outputFS.delete(path, false);
//      }
      outputStream = outputFS.create(path);
      byte[] inputBytes = new byte[BLOCK_SIZE];
      int inputBytesRead;
      for (Path blockPath : blockFiles) {
        if (!blocksFS.exists(blockPath)) {
          LOG.error("Missing block {} of file {}", blockPath, fileName);
          throw new RuntimeException("Missing block: " + blockPath);
        }
        DataInputStream is = new DataInputStream(blocksFS.open(blockPath));
        while ((inputBytesRead = is.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
        is.close();
      }
      if (deleteSubFiles) {
        for (Path blockPath : blockFiles) {
          blocksFS.delete(blockPath, false);
        }
      }
    } catch (IOException ex) {
      try {
        if (outputFS.exists(path)) {
          outputFS.delete(path, false);
        }
      } catch (IOException e) {
      }
      throw new RuntimeException(ex);
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to close output stream.", e);
      }
    }
  }

  private void stitchAndAppend(FileSplitter.FileMetadata fileMetadata) throws IOException
  {
    /*
     * 1. File is smaller than the block size, just move to destination folder and rename
     * 2. File = one block, same as 1 above
     * 3.
     */
    String fileName = fileMetadata.getFileName();
    LOG.debug(" filePath {}/{}", filePath, fileName);
    Path outputFilePath = new Path(filePath, fileName);

//    boolean lastBlockSmaller = isLastBlockSmaller(fileMetadata);
    // Stitch n-1 blocks
    //
    int numBlocks = fileMetadata.getNumberOfBlocks();

    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksPath, Long.toString(blocksArray[0]));// The first incomplete block.

    // Should we check if the last block is also = HDFS block size?
    Path lastBlock = new Path(blocksPath, Long.toString(blocksArray[numBlocks - 1]));// The last incomplete block.
    Path[] blockFiles ;
    DataInputStream inputStream = null;
    FSDataOutputStream outputStream = null;
    
    try{
    if(numBlocks > 2){
      
    blockFiles = new Path[numBlocks - 2]; // Leave the first and the last block.

    for (int index = 1; index < numBlocks - 1; index++) { // Except the first and last block
      blockFiles[index - 1] = new Path(blocksPath, Long.toString(blocksArray[index]));
    }
    outputFS.concat(firstBlock, blockFiles);
    }
    
    //Append the last block

      // Append the last block
      byte[] inputBytes = new byte[1024 * 64]; // 64 KB
      int inputBytesRead;

      outputStream = outputFS.append(firstBlock);
      inputStream = new DataInputStream(blocksFS.open(lastBlock));
      while ((inputBytesRead = inputStream.read(inputBytes)) != -1) {
        outputStream.write(inputBytes, 0, inputBytesRead);
      }
      outputStream.flush();
//      outputStream.close();

      // n-1 block were stitched. the last block needs to be explicitly deleted.
      blocksFS.delete(lastBlock, false);
    } catch (IOException e) {
      LOG.error("Exception in StitchAndAppend.", e);
      throw new RuntimeException(e);
    } finally {
      if (inputStream != null) {
        LOG.info("closing input stream.");
        inputStream.close();
        inputStream=null;
      }
      if (outputStream != null) {
        LOG.info("closing output stream.");
        outputStream.close();
        outputStream=null;
      }
    }
    
    moveFile(firstBlock,outputFilePath);
  }

  private boolean matchBlockSize(FileSplitter.FileMetadata fileMetadata, long defaultBlockSize)
  {
    try {
      Path firstBlockPath = new Path(blocksPath + "/" + fileMetadata.getBlockIds()[0]);
      if (!blocksFS.exists(firstBlockPath)) {
        LOG.error("Missing block {} of file {}", firstBlockPath, fileMetadata.getFileName());
        throw new RuntimeException("Missing block: " + firstBlockPath);
      }

      FileStatus status = blocksFS.getFileStatus(firstBlockPath);
      if (status.getLen() % defaultBlockSize == 0) { // Equal or multiple of HDFS block size
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to verify if reader block size and HDFS block size is same.");
    }
    return false;
  }

  private boolean moveFile(Path src, Path dst){
    // Move the file to right destination.
    boolean moveSuccessful;
    try {
      moveSuccessful = outputFS.rename(src, dst);
    } catch (IOException e) {
      throw new RuntimeException("Failed to move file to destination folder." ,e);
    }
    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      LOG.debug("File {} move to destination folder failed.", dst);
      
    }
    return moveSuccessful;

  }
//  private boolean isLastBlockSmaller(FileSplitter.FileMetadata fileMetadata){
//    try {
//    Path lastBlockPath = new Path(blocksPath + "/" + fileMetadata.getBlockIds()[fileMetadata.getNumberOfBlocks()-1]);
//
//    if (!blocksFS.exists(lastBlockPath)) {
//      LOG.error("Missing block {} of file {}", lastBlockPath, fileMetadata.getFileName());
//      throw new RuntimeException("Missing block: " + lastBlockPath);
//    }
//
//    FileStatus status = blocksFS.getFileStatus(lastBlockPath);
//    if (status.getLen() % defaultBlockSize == 0) { // Equal or multiple of HDFS block size
//      return true;
//    }
//    }
//  }

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }

}
