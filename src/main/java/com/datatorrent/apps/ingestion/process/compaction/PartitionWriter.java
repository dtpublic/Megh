/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.DataInputStream;
import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * An operator used in compaction for writing contents of partition from given partition metadata.
 * PartitionMetaData specifies which file blocks should be used to write contents of this partition.
 */

public class PartitionWriter extends AbstractReconciler<PatitionMetaData, PatitionMetaData>
{
  protected transient FileSystem appFS, outputFS;

  /**
   * Output directory where partition files are written
   */
  @NotNull
  private String outputDir;
  
  /**
   * Blocks directory from where blocks are read
   */
  transient private String blocksDir;
  
  /**
   * Suffix used for naming partition files for which are under progress
   */
  private static final String TMP_FILE_SUFFIX = ".tmp";
  
  /** 
   * Buffer size to be used for reading data from block files
   */
  private static final int BUFFER_SIZE = 64 * 1024;

  /**
   * Initialize  outputFS, appFS
   * @see com.datatorrent.lib.io.fs.AbstractReconciler#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    blocksDir = context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
    
    try {
      outputFS = getFSInstance(outputDir);

    } catch (IOException ex) {
      throw new RuntimeException("Exception in getting output file system.", ex);
    }
    try {
      appFS = getFSInstance(blocksDir);
    } catch (IOException ex) {
      try {
        outputFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception in closing output file system.", e);
      }
      throw new RuntimeException("Exception in getting application file system setup.", ex);
    }
  }

  /**
   * Close appFS, outputFS
   */
  @Override
  public void teardown()
  {
    super.teardown();
    boolean gotException = false;
    try {
      if (appFS != null) {
        appFS.close();
        appFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }

    try {
      if (outputFS != null) {
        outputFS.close();
        outputFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }
    if (gotException) {
      throw new RuntimeException("Exception while closing file systems.");
    }
  }

  protected FileSystem getFSInstance(String dir) throws IOException
  {
    return FileSystem.newInstance((new Path(dir)).toUri(), new Configuration());
  }

  /**
   * Process tuple will enque the tuple into reconsiler queue
   * @see com.datatorrent.lib.io.fs.AbstractReconciler#processTuple(java.lang.Object)
   */
  @Override
  protected void processTuple(PatitionMetaData partitionMetadata)
  {
    enqueueForProcessing(partitionMetadata);
  }

  /**
   * Write partition meta data into temporary file
   * and then move to final file after complettion
   * 
   * @see com.datatorrent.lib.io.fs.AbstractReconciler#processCommittedData(java.lang.Object)
   */
  @Override
  protected void processCommittedData(PatitionMetaData partitionMetadata)
  {
    Path tempPartitionFilePath;
    try {
      tempPartitionFilePath = writeTempPartitionFile(partitionMetadata);
      moveToFinalPartitionFile(tempPartitionFilePath, partitionMetadata);
      completedPartitionMetaDataOutputPort.emit(partitionMetadata);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 
   * @param tempPartitionFilePath
   * @param partitionMetadata
   * @throws IOException
   */
  private void moveToFinalPartitionFile(Path tempPartitionFilePath, PatitionMetaData partitionMetadata) throws IOException
  {
    Path destination = new Path(outputDir, partitionMetadata.getPartFileName());
    Path src = Path.getPathWithoutSchemeAndAuthority(tempPartitionFilePath);
    Path dst = Path.getPathWithoutSchemeAndAuthority(destination);
    
    boolean moveSuccessful = false;
    if (!outputFS.exists(dst.getParent())) {
      outputFS.mkdirs(dst.getParent());
    }
    if (outputFS.exists(dst)) {
      outputFS.delete(dst, false);
    }
    moveSuccessful = outputFS.rename(src, dst);

    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      throw new RuntimeException("Unable to move file from " + src + " to " + dst);
    }
  }

  /**
   * Read data from block files and write to partition file.
   * Information about which block files should be read is specified in partitionMetadata
   * @param partitionMetadata
   * @return
   * @throws IOException
   */
  private Path writeTempPartitionFile(PatitionMetaData partitionMetadata) throws IOException
  {
    FSDataOutputStream outputStream = null;
    try {
      Path tempPartitionFilePath = new Path(outputDir, partitionMetadata.getPartFileName() + TMP_FILE_SUFFIX);
      LOG.debug("outputFS={}",outputFS);
      outputStream = outputFS.create(tempPartitionFilePath);
      for (FileBlockMetadata fileBlockMetadata : partitionMetadata.getBlockMetaDataList()) {
        writeFileBlock(outputStream, fileBlockMetadata);
      }
      return tempPartitionFilePath;
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }

  /**
   * Write a particular file block into partition file
   * @param outputStream
   * @param fileBlockMetadata
   * @throws IOException
   */
  private void writeFileBlock(FSDataOutputStream outputStream, FileBlockMetadata fileBlockMetadata) throws IOException
  {
    DataInputStream inStream = null;
    try {
      byte[] buffer = new byte[BUFFER_SIZE];
      int inputBytesRead;
      Path blockPath = new Path(fileBlockMetadata.getFilePath());
      if (!appFS.exists(blockPath)) {
        throw new RuntimeException("Exception: Missing block " + blockPath);
      }
      inStream = new DataInputStream(appFS.open(blockPath));
      inStream.skip(fileBlockMetadata.getOffset());

      long bytesRemainingToRead = fileBlockMetadata.getLength();
      int bytesToread = Math.min(BUFFER_SIZE, (int) bytesRemainingToRead);
      while (((inputBytesRead = inStream.read(buffer, 0, bytesToread)) != -1) && bytesRemainingToRead > 0) {
        outputStream.write(buffer, 0, inputBytesRead);
        bytesRemainingToRead -= inputBytesRead;
        bytesToread = Math.min(BUFFER_SIZE, (int) bytesRemainingToRead);
      }
    } finally {
      if (inStream != null) {
        inStream.close();
      }
    }
  }

  public String getOutputDir()
  {
    return outputDir;
  }

  public void setOutputDir(String outputDir)
  {
    this.outputDir = outputDir;
  }

  private static final Logger LOG = LoggerFactory.getLogger(PartitionWriter.class);
  
  /**
   * Output port emits one tuple per partition after contents of that partition are written to appFS 
   */
  public final transient DefaultOutputPort<PatitionMetaData> completedPartitionMetaDataOutputPort = new DefaultOutputPort<PatitionMetaData>();
}
