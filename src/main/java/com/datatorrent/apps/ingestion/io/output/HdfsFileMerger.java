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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  protected transient FileSystem fs;
  @NotNull
  protected String filePath;
  @NotNull
  protected String blocksPath;

  private String applicationId;
  private boolean deleteSubFiles = true;

  private static final int  BLOCK_SIZE = 1024 * 1024 * 64; // 64 MB

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
    if (applicationId == null) {
      applicationId = context.getValue(DAG.APPLICATION_ID);
      filePath = filePath + "/" + applicationId;
      blocksPath= blocksPath + "/" + applicationId;
    }
    try {
      fs = FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
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
      if (fs != null) {
        fs.close();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    fs = null;
  }

  private void mergeFiles(FileSplitter.FileMetadata fileMetadata)
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
    try {
      if (fs.exists(path)) {
        fs.delete(path, false);
      }
      FSDataOutputStream outputStream = fs.create(path);
      byte[] inputBytes = new byte[BLOCK_SIZE];
      int inputBytesRead;
      for (Path blockPath : blockFiles) {
        if (!fs.exists(blockPath)) {
          LOG.error("Missing block {} of file {}", blockPath, fileName);
          throw new RuntimeException("Missing block: " + blockPath);
        }
        DataInputStream is = new DataInputStream(fs.open(blockPath));
        while ((inputBytesRead = is.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
        is.close();
      }
      outputStream.close();
      if (deleteSubFiles) {
        for (Path blockPath : blockFiles) {
          fs.delete(blockPath, false);
        }
      }
    } catch (IOException ex) {
      try {
        if (fs.exists(path)) {
          fs.delete(path, false);
        }
      } catch (IOException e) {
      }
      throw new RuntimeException(ex);
    }
  }

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }
  public String getBlocksPath()
  {
    return blocksPath;
  }

  public void setBlocksPath(String blocksPath)
  {
    this.blocksPath = blocksPath;
  }
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileMerger.class);
}
