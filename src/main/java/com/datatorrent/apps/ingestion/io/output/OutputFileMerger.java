/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;
import java.io.OutputStream;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.ingestion.IngestionConstants.IngestionCounters;
import com.datatorrent.apps.ingestion.common.BlockNotFoundException;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.malhar.lib.counters.BasicCounters;

/**
 * This is generic File Merger which can be used to merge data from different files into single output file.
 * OutputFileMetaData defines constituents of the output file.
 */
public class OutputFileMerger<T extends OutputFileMetaData> extends AbstractReconciler<T,T>
{
  protected transient FileSystem appFS, outputFS;

  @NotNull
  protected String filePath;
  transient protected String blocksDir;
  
  protected final BasicCounters<MutableLong> mergerCounters;
  protected transient Context.OperatorContext context;


  protected static final String PART_FILE_EXTENTION = "._COPYING_";


  public final transient DefaultOutputPort<T> completedFilesMetaOutput = new DefaultOutputPort<T>();
  
  public OutputFileMerger()
  {
    mergerCounters = new BasicCounters<MutableLong>(MutableLong.class);
  }

  
  @Override
  public void setup(Context.OperatorContext context)
  {
    this.context = context;
    mergerCounters.setCounter(IngestionCounters.TOTAL_BYTES_WRITTEN_AFTER_COMPRESSION, new MutableLong());
    mergerCounters.setCounter(IngestionCounters.TIME_TAKEN_FOR_ENCRYPTION, new MutableLong());
    mergerCounters.setCounter(Counters.TOTAL_DATA_INGESTED, new MutableLong());
    

    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;

    try {
      outputFS = getOutputFSInstance();
    } catch (IOException ex) {
      throw new RuntimeException("Exception in getting output file system.", ex);
    }
    try {
      appFS = getAppFSInstance();
    } catch (IOException ex) {
      try {
        outputFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception in closing output file system.", e);
      }
      throw new RuntimeException("Exception in getting application file system.", ex);
    }

    super.setup(context); // Calling it at the end as the reconciler thread uses resources allocated above.
  }
  
  /* 
   * Calls super.endWindow() and sets counters 
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    context.setCounters(mergerCounters);
  }

  protected FileSystem getAppFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(blocksDir)).toUri(), new Configuration());
  }

  protected FileSystem getOutputFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
  }


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

  @Override
  protected void processTuple(T fileMetadata)
  {
    enqueueForProcessing(fileMetadata);
  }

  @Override
  protected void processCommittedData(T queueInput)
  {
    try {
      mergeOutputFile(queueInput);
    } catch (IOException e) {
      throw new RuntimeException("Unable to merge file: " + queueInput.getOutputRelativePath(), e);
    }
  }

  /**
   * Read data from block files and write to output file.
   * Information about which block files should be read is specified in outFileMetadata
   * @param outFileMetadata
   * @return
   * @throws IOException
   */

  protected void mergeOutputFile(T outFileMetadata) throws IOException
  {
    mergeBlocks(outFileMetadata);
    completedFilesMetaOutput.emit(outFileMetadata);
    LOG.debug("Completed processing file: {} ", outFileMetadata.getOutputRelativePath());
  }
  
  protected void mergeBlocks(T outFileMetadata) throws IOException
  {
    try {
      writeTempOutputFile(outFileMetadata);
      moveToFinalFile(outFileMetadata);
    } catch (BlockNotFoundException e) {
      LOG.info("Block file {} not found. Assuming recovery mode for file {}. ", e.getBlockPath(), outFileMetadata.getOutputRelativePath());
      //Remove temp output file
      Path tempOutFilePath = new Path(filePath, outFileMetadata.getOutputRelativePath() + PART_FILE_EXTENTION);
      outputFS.delete(tempOutFilePath, false);
    }
  }
  
  protected OutputStream writeTempOutputFile(T outFileMetadata) throws IOException, BlockNotFoundException{
    Path tempOutFilePath = new Path(filePath, outFileMetadata.getOutputRelativePath() + PART_FILE_EXTENTION);
    OutputStream outputStream = getOutputStream(tempOutFilePath);
    try {
      for (OutputBlock outputBlock : outFileMetadata.getOutputBlocksList()) {
        outputBlock.writeTo(appFS, blocksDir, outputStream);
      }
    } finally {
      outputStream.close();
    }
    return outputStream;
  }
  
  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    return outputFS.create(partFilePath);
  }

  protected void moveToFinalFile(T outFileMetadata) throws IOException
  {
    Path tempOutFilePath = new Path(filePath, outFileMetadata.getOutputRelativePath() + PART_FILE_EXTENTION);
    Path destination = new Path(filePath, outFileMetadata.getOutputRelativePath());
    moveToFinalFile(tempOutFilePath, destination);
  }
  
  /**
   * 
   * @param tempPartitionFilePath
   * @param partitionMetadata
   * @throws IOException
   */
  protected void moveToFinalFile(Path tempOutFilePath, Path destination) throws IOException
  {
    Path src = Path.getPathWithoutSchemeAndAuthority(tempOutFilePath);
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
      long outFileLength = outputFS.getFileStatus(dst).getLen();
      mergerCounters.getCounter(IngestionCounters.TOTAL_BYTES_WRITTEN_AFTER_COMPRESSION).add(outFileLength);
      
    } else {
      throw new RuntimeException("Unable to move file from " + src + " to " + dst);
    }
  }
  

  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }
  
  public String getFilePath()
  {
    return filePath;
  }
  
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }
  
  public static enum Counters
  {
    TOTAL_DATA_INGESTED;
  }


  private static final Logger LOG = LoggerFactory.getLogger(OutputFileMerger.class);
}
