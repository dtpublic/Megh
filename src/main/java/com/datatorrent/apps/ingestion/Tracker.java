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
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;

public class Tracker extends BaseOperator
{

  ConcurrentHashMap<String, Boolean> fileSet = new ConcurrentHashMap<String, Boolean>();

  private transient int timeoutWindowCount;

  private int idleCount ;
  private boolean noActivity ;
  private transient String oneTimeCopySignal;
  private transient FileSystem appFS;
  private boolean fileCreated ;
  private boolean oneTimeCopy ;

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
      } else {
        // With reconciler based FileMerger, this is not possible.
        throw new RuntimeException("Tuple from FileMerger came before tuple from FileSplitter");
      }
      LOG.debug("File copied successfully: {}", tuple.getFilePath());
    }
  };

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
    oneTimeCopySignal = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + Application.ONE_TIME_COPY_DONE_FILE;
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

  private static final Logger LOG = LoggerFactory.getLogger(Tracker.class);
}
