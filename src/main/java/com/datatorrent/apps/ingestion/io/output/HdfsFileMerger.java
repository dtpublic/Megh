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

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }

}
