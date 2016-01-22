/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Queues;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputBlock;

/**
 * This is generic File Merger which can be used to merge data from different
 * files into single output file. OutputFileMetaData defines constituents of the
 * output file.
 *
 */
public class FileStitcher<T extends OutputFileMetaData> extends AbstractReconciler<T, T>
{
  protected transient FileSystem appFS, outputFS;

  @NotNull
  protected String filePath;
  transient protected String blocksDir;

  protected transient Context.OperatorContext context;

  protected static final String PART_FILE_EXTENTION = "._COPYING_";

  protected Queue<T> successfulFiles = Queues.newLinkedBlockingQueue();
  protected Queue<T> skippedFiles = Queues.newLinkedBlockingQueue();
  protected Queue<T> failedFiles = Queues.newLinkedBlockingQueue();

  public final transient DefaultOutputPort<T> completedFilesMetaOutput = new DefaultOutputPort<T>();
  private boolean writeChecksum = true;
  transient Path tempOutFilePath;

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.context = context;

    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;

    try {
      outputFS = getOutputFSInstance();
      outputFS.setWriteChecksum(writeChecksum);
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
    T tuple;
    int size = doneTuples.size();
    for (int i = 0; i < size; i++) {
      tuple = doneTuples.peek();
      // If a tuple is present in doneTuples, it has to be also present in successful/failed/skipped
      // as processCommittedData adds tuple in successful/failed/skipped
      // and then reconciler thread add that in doneTuples 
      if (successfulFiles.contains(tuple)) {
        successfulFiles.remove(tuple);
        LOG.debug("File copy successful: {}", tuple.getOutputRelativePath());
      } else if (skippedFiles.contains(tuple)) {
        skippedFiles.remove(tuple);
        LOG.debug("File copy skipped: {}", tuple.getOutputRelativePath());
      } else if (failedFiles.contains(tuple)) {
        failedFiles.remove(tuple);
        LOG.debug("File copy failed: {}", tuple.getOutputRelativePath());
      } else {
        throw new RuntimeException(
            "Tuple present in doneTuples but not in successfulFiles: " + tuple.getOutputRelativePath());
      }
      completedFilesMetaOutput.emit(tuple);
      committedTuples.remove(tuple);
      doneTuples.poll();
    }
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
   * Read data from block files and write to output file. Information about
   * which block files should be read is specified in outFileMetadata
   * 
   * @param outFileMetadata
   * @return
   * @throws IOException
   */

  protected void mergeOutputFile(T outFileMetadata) throws IOException
  {
    mergeBlocks(outFileMetadata);
    successfulFiles.add(outFileMetadata);
    LOG.debug("Completed processing file: {} ", outFileMetadata.getOutputRelativePath());
  }

  protected void mergeBlocks(T outFileMetadata) throws IOException
  {
    //when writing to tmp files there can be vagrant tmp files which we have to clean
    final Path dst = new Path(filePath, outFileMetadata.getOutputRelativePath());
    PathFilter tempFileFilter = new PathFilter()
    {
      @Override
      public boolean accept(Path path)
      {
        return path.getName().startsWith(dst.getName()) && path.getName().endsWith(PART_FILE_EXTENTION);
      }
    };
    if (outputFS.exists(dst.getParent())) {
      FileStatus[] statuses = outputFS.listStatus(dst.getParent(), tempFileFilter);
      for (FileStatus status : statuses) {
        String statusName = status.getPath().getName();
        LOG.debug("deleting vagrant file {}", statusName);
        outputFS.delete(status.getPath(), true);
      }
    }
    tempOutFilePath = new Path(filePath,
        outFileMetadata.getOutputRelativePath() + '.' + System.currentTimeMillis() + PART_FILE_EXTENTION);
    try {
      writeTempOutputFile(outFileMetadata);
      moveToFinalFile(outFileMetadata);
    } catch (BlockNotFoundException e) {
      LOG.info("Block file {} not found. Assuming recovery mode for file {}. ", e.getBlockPath(),
          outFileMetadata.getOutputRelativePath());
      //Remove temp output file
      outputFS.delete(tempOutFilePath, false);
    }
  }

  protected OutputStream writeTempOutputFile(T outFileMetadata) throws IOException, BlockNotFoundException
  {
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
    this.filePath = ModuleUtils.convertSchemeToLowerCase(filePath);
  }

  public boolean isWriteChecksum()
  {
    return writeChecksum;
  }

  public void setWriteChecksum(boolean writeChecksum)
  {
    this.writeChecksum = writeChecksum;
  }

  public static enum Counters
  {
    TOTAL_DATA_INGESTED;
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileStitcher.class);
}
