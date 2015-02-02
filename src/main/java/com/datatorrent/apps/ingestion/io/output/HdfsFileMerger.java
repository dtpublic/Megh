/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.DataInputStream;
import java.io.IOException;

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
    
  boolean dfsAppendSupport = Boolean.getBoolean(outputFS.getConf().get("dfs.support.append"));
  long defaultBlockSize = outputFS.getDefaultBlockSize(new Path(fileMetadata.getFilePath()));
  boolean sameSize = matchBlockSize(fileMetadata,defaultBlockSize);
  LOG.debug("Append flag is: {}",dfsAppendSupport);
  LOG.debug("defaultBlockSize flag/size is: {}",defaultBlockSize);
  LOG.debug("sameSize flag is: {}",sameSize);
  LOG.debug("Output scheme flag is: {}",outputFS.getScheme());
//  LOG.debug("instanceof flag is: {}",(outputFS instanceof DistributedFileSystem));
//    if(sameSize && dfsAppendSupport && outputFS instanceof DistributedFileSystem){
      if(sameSize){
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
      if (outputFS.exists(path)) {
        outputFS.delete(path, false);
      }
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
    String fileName = fileMetadata.getFileName();
    LOG.debug(" filePath {}/{}", filePath, fileName);
    Path outputFilePath = new Path(filePath, fileName);

    // Stitch n-1 blocks
    //
    int numBlocks = fileMetadata.getNumberOfBlocks();

    Path[] blockFiles = new Path[numBlocks - 2]; // Leave the first and the last block.
    long[] blocksArray = fileMetadata.getBlockIds();

    for (int index = 1; index < numBlocks - 1; index++) { // Except the first and last block
      blockFiles[index - 1] = new Path(blocksPath, Long.toString(blocksArray[index]));
    }
    Path firstBlock = new Path(blocksPath, Long.toString(blocksArray[0]));// The first incomplete block.

    // Should we check if the last block is also = HDFS block size?
    Path lastBlock = new Path(blocksPath, Long.toString(blocksArray[numBlocks - 1]));// The last incomplete block.

    // Stitch n-1 block
    DataInputStream inputStream = null;
    FSDataOutputStream outputStream = null;
    try {
      outputFS.concat(firstBlock, blockFiles);

      // Append the last block
      byte[] inputBytes = new byte[1024 * 64]; // 64 KB
      int inputBytesRead;

      outputStream = outputFS.append(firstBlock);
      inputStream = new DataInputStream(blocksFS.open(lastBlock));
      while ((inputBytesRead = inputStream.read(inputBytes)) != -1) {
        outputStream.write(inputBytes, 0, inputBytesRead);
      }
      outputStream.flush();
      outputStream.close();
      // Move the file to right destination.
      boolean moveSuccessful = outputFS.rename(firstBlock, outputFilePath);
      LOG.info("File move success: {}", fileName);
      if (moveSuccessful) {
        LOG.debug("File moved successfully to : {}", outputFilePath);
      } else {
        LOG.debug("File move failed : {}", fileName);
        throw new RuntimeException("Failed to copy file: " + fileName);
      }
    } catch (IOException e) {
      LOG.error("Exception in StitchAndAppend.", e);
      throw new RuntimeException(e);
    } finally {
      if (inputStream != null) {
        LOG.info("closing input stream.");
        inputStream.close();
        inputStream=null;
      }
    }
  }

  private boolean matchBlockSize(FileSplitter.FileMetadata fileMetadata, long defaultBlockSize)
  {
    try {
      Path blockPath = new Path(blocksPath + "/" + fileMetadata.getBlockIds()[0]);
      if (!blocksFS.exists(blockPath)) {
        LOG.error("Missing block {} of file {}", blockPath, fileMetadata.getFileName());
        throw new RuntimeException("Missing block: " + blockPath);
      }

      FileStatus status = blocksFS.getFileStatus(blockPath);
      if (status.getLen() % defaultBlockSize == 0) { // Equal or multiple of HDFS block size
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to verify if reader block size and HDFS block size is same.");
    }
    return false;
  }

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }

}
