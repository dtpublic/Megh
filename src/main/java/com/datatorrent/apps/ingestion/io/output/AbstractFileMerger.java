/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.google.common.annotations.VisibleForTesting;

/**
 * This merges the various small block files to the main file
 * 
 * @author Sandeep
 */

public class AbstractFileMerger extends BaseOperator
{
  protected transient FileSystem blocksFS, outputFS;

  @NotNull
  private String outputDir;
  private String blocksDir;

  private boolean deleteSubFiles;
  private boolean overwriteOutputFile;

  private long defaultBlockSize;
  private List<String> skippedFiles;

  private static final String PART_FILE_EXTENTION = ".part";
  private int bufferSize = 64 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileMerger.class);

  public AbstractFileMerger()
  {
    deleteSubFiles = true;
    skippedFiles = new ArrayList<String>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    blocksDir = context.getValue(DAG.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS;
    try {
      outputFS = FileSystem.newInstance((new Path(outputDir)).toUri(), new Configuration());
      blocksFS = FileSystem.newInstance((new Path(blocksDir)).toUri(), new Configuration());
      setDefaultBlockSize(outputFS.getDefaultBlockSize(new Path(outputDir)));

    } catch (IOException ex) {
      LOG.error("Exception in FileMerger setup.", ex);
      throw new RuntimeException(ex);
    }
  }

  public final transient DefaultInputPort<IngestionFileSplitter.IngestionFileMetaData> processedFileInput = new DefaultInputPort<IngestionFileSplitter.IngestionFileMetaData>() {
    @Override
    public void process(IngestionFileSplitter.IngestionFileMetaData fileMetadata)
    {
      mergeFile(fileMetadata);
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

  public void mergeFile(FileSplitter.FileMetadata fmd)
  {

  }

  @VisibleForTesting
  protected void moveFile(Path src, Path dst)
  {
    boolean moveSuccessful;
    try {
      if (!outputFS.exists(dst.getParent())) {
        outputFS.mkdirs(dst.getParent());
      }
      if (outputFS.exists(dst)) {
        outputFS.delete(dst, false);
      }
      moveSuccessful = outputFS.rename(src, dst);
    } catch (IOException e) {
      LOG.error("File move failed from {} to {} ", src, dst, e);
      throw new RuntimeException("Failed to move file to destination folder.", e);
    }
    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      LOG.error("Move file {} to {} failed.", src, dst);
      throw new RuntimeException("Moving file to output folder failed.");
    }
  }

  public List<String> getSkippedFilesList()
  {
    return skippedFiles;
  }

  public String getFilePath()
  {
    return outputDir;
  }

  public void setFilePath(String outputDir)
  {
  }

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }

  /**
   * @return the defaultBlockSize
   */
  public long getDefaultBlockSize()
  {
    return defaultBlockSize;
  }

  /**
   * @param defaultBlockSize
   *          the defaultBlockSize to set
   */
  public void setDefaultBlockSize(long defaultBlockSize)
  {
    this.defaultBlockSize = defaultBlockSize;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }
}
