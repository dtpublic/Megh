package com.datatorrent.apps.ingestion;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;

public class Tracker extends BaseOperator
{

  ConcurrentHashMap<String, Boolean> fileSet = new ConcurrentHashMap<String, Boolean>();

  private transient int timeoutWindowCount;

  private int idleCount ;
  private boolean noActivity ;
  transient String oneTimeCopySignal;
  protected transient FileSystem appFS;
  private boolean fileCreated ;
  private boolean oneTimeCopy ;
  protected String blocksDir;
  

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    // How long should we Wait for safe deletion of blocks from FileMerger.
    timeoutWindowCount = context.getValue(DAGContext.CHECKPOINT_WINDOW_COUNT);
    
    try {
      appFS = getFileSystem(context);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    
    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
    oneTimeCopySignal = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + Application.ONE_TIME_COPY_DONE_FILE;
  }
  

  public final transient DefaultInputPort<FileMetadata> inputFileSplitter = new DefaultInputPort<FileMetadata>() {

    @Override
    public void process(FileMetadata tuple)
    {
      noActivity = false;
      fileSet.put(tuple.getFilePath(), true);
    }
  };

  public final transient DefaultInputPort<FileMetadata> inputFileMerger = new DefaultInputPort<FileMetadata>() {

    @Override
    public void process(FileMetadata tuple)
    {
      noActivity = false;
      if (fileSet.get(tuple.getFilePath())) {
        fileSet.remove(tuple.getFilePath());
        deleteBlockFiles(tuple);
      } else {
        // With reconciler based FileMerger, this is not possible.
        throw new RuntimeException("Tuple from FileMerger came before tuple from FileSplitter");
      }
      LOG.debug("File copied successfully: {}", tuple.getFilePath());
    }
  };

  /**
   * @param tuple
   */
  protected void deleteBlockFiles(FileMetadata fileMetadata)
  {
    if (fileMetadata.isDirectory()) {
      return;
    }

    for (long blockId : fileMetadata.getBlockIds()) {
      Path blockPath = new Path(blocksDir, Long.toString(blockId));
      try {
        if (appFS.exists(blockPath)) { // takes care if blocks are deleted and then the operator is redeployed.
          appFS.delete(blockPath, false);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to delete block: " + blockId, e);
      }
    }
  }

  private FileSystem getFileSystem(OperatorContext context) throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  public void beginWindow(long windowId)
  {
    noActivity = true;
  }

  @Override
  public void endWindow()
  {
    if (noActivity && fileSet.isEmpty()) {
      idleCount++;
    } else {
      idleCount = 0;
    }
    if (idleCount > timeoutWindowCount) {
      try {
        sendShutdownSignal();
      } catch (IOException e) {
        throw new RuntimeException("Unable to send shutdown signal.", e);
      }
    }
  }

  private void sendShutdownSignal() throws IOException
  {
    if (fileCreated) {
      return;
    }
    fileCreated = true;
    FSDataOutputStream stream = appFS.create(new Path(oneTimeCopySignal), true);
    stream.close();
    LOG.debug("One time copy completed. Writing file: {}", oneTimeCopySignal);
  }

  /**
   * @return the oneTimeCopy
   */
  public boolean isOneTimeCopy()
  {
    return oneTimeCopy;
  }

  /**
   * @param oneTimeCopy
   *          the oneTimeCopy to set
   */
  public void setOneTimeCopy(boolean oneTimeCopy)
  {
    this.oneTimeCopy = oneTimeCopy;
  }

  /**
   * @return the timeoutWindowCount
   */
  public int getTimeoutWindowCount()
  {
    return timeoutWindowCount;
  }

  /**
   * @param timeoutWindowCount
   *          the timeoutWindowCount to set
   */
  public void setTimeoutWindowCount(int timeoutWindowCount)
  {
    this.timeoutWindowCount = timeoutWindowCount;
  }
  
  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }

  private static final Logger LOG = LoggerFactory.getLogger(Tracker.class);
}
