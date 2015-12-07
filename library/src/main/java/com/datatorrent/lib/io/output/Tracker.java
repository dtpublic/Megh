package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.input.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;
import com.datatorrent.lib.io.output.TrackerEvent.TrackerEventType;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * <p>Tracker class.</p>
 *
 * @since 1.0.0
 */
public class Tracker extends IdleWindowCounter
{
  
  public static final String ONE_TIME_COPY_DONE_FILE = "IngestionApp.complete";
  
  Map<String, MutableInt> fileMap = new HashMap<String, MutableInt>();
  transient String oneTimeCopySignal;
  protected transient FileSystem appFS;
  private boolean fileCreated ;
  private boolean oneTimeCopy ;
  protected String blocksDir;
  Map<String, ExtendedModuleFileMetaData> metricsFileMap = new HashMap<String, ExtendedModuleFileMetaData>();
  CircularFifoBuffer queue = new CircularFifoBuffer(100);

  private static final String STATUS_METRICS_STATUS = "Status";
  private static final String STATUS_METRICS_COUNT = "Count";
  private static final String STATUS_METRICS_TOTAL_SIZE = "Total Size (MB)";
  
  private static final String FILEDETAILS_METRICS_NAME = "Name";
  private static final String FILEDETAILS_METRICS_OUT_PATH = "Output Path";
  private static final String FILEDETAILS_METRICS_SIZE = "Size (MB)";
  private static final String FILEDETAILS_METRICS_COMPR_RATIO = "Compression Ratio";
  private static final String FILEDETAILS_METRICS_COMPR_TIME = "Compression Time (sec)";
  private static final String FILEDETAILS_METRICS_ENC_TIME = "Encryption Time (sec)";
  private static final String FILEDETAILS_METRICS_OVERALL_TIME = "Overall Time (sec)";
  private static final String FILEDETAILS_METRICS_STATUS = "Status";
  
  
  private static final int DEFAULT_TIMEOUT_WINDOW_COUNT = 120;
  
  public final transient DefaultOutputPort<TrackerEvent> trackerEventOutPort = new DefaultOutputPort<TrackerEvent>();
  
  public final transient DefaultOutputPort<List<Map<String, Object>>> statusMetrics = new DefaultOutputPort<List<Map<String, Object>>>();
  public final transient DefaultOutputPort<List<Map<String, Object>>> fileDetailsMetrics = new DefaultOutputPort<List<Map<String,Object>>>();
  
  @AutoMetric
  private int remainingFileCounts;
  
  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {   
    try {
      appFS = getFileSystem(context);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    
    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
    oneTimeCopySignal = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + ONE_TIME_COPY_DONE_FILE;
  }
  

  public final transient DefaultInputPort<FileMetadata> inputFileSplitter = new DefaultInputPort<FileMetadata>() {

    @Override
    public void process(FileMetadata tuple)
    {
      markActivity();
      incrementFileCount(new ExtendedModuleFileMetaData((ModuleFileMetaData)tuple));
      LOG.debug("Received tuple from FileSplitter: {}", tuple.getFilePath());
    }
  };

  public final transient DefaultInputPort<ExtendedModuleFileMetaData> inputFileMerger = new DefaultInputPort<ExtendedModuleFileMetaData>() {

    @Override
    public void process(ExtendedModuleFileMetaData tuple)
    {
      markActivity();
      deleteBlockFiles(tuple);
      decrementFileCount(tuple);
      LOG.debug("File copied successfully: {}", tuple.getFilePath());
    }
  };
  
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    remainingFileCounts = 0;
  }

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

  /**
   * Check if tracker has more files in progress  
   * @see com.datatorrent.apps.ingestion.common.IdleWindowCounter#hasMoreWork()
   */
  @Override
  protected boolean hasMoreWork()
  {
    return !fileMap.isEmpty();
  }
  
  /**
   * Send Shutdown signal if idle Window Threshold is Reached
   * @see com.datatorrent.apps.ingestion.common.IdleWindowCounter#idleWindowThresholdReached()
   */
  @Override
  protected void idleWindowThresholdReached()
  {
    try {
      sendShutdownSignal();
    } catch (IOException e) {
      throw new RuntimeException("Unable to send shutdown signal.", e);
    }
  }
  
  @Override
  protected int getIdleWindowThresholdDefault()
  {
    return DEFAULT_TIMEOUT_WINDOW_COUNT;
  }

  private void sendShutdownSignal() throws IOException
  {
    if (fileCreated) {
      return;
    }
    fileCreated = true;
    FSDataOutputStream stream = appFS.create(new Path(oneTimeCopySignal), true);
    stream.close();
    LOG.info("One time copy completed. Sending shutdown signal via file: {}", oneTimeCopySignal);
  }

  private void incrementFileCount(ExtendedModuleFileMetaData tuple)
  {
    String filePath = tuple.getFilePath();
    MutableInt count = fileMap.get(filePath);
    if (count == null) {
      fileMap.put(filePath, new MutableInt(1));
    } else {
      count.increment();
    }
    metricsFileMap.put(filePath, tuple);
    if (!queue.contains(filePath)) {
      queue.add(filePath);
    }
    LOG.debug("Adding: file: {}, map size: {}", tuple, fileMap.size());
  }

  private void decrementFileCount(ExtendedModuleFileMetaData tuple)
  {
    String filePath = tuple.getFilePath(); 
    MutableInt count = fileMap.get(filePath);
    if (count == null) {
      throw new RuntimeException("Tuple from FileMerger came before tuple from FileSplitter: " + tuple);
    }
    count.decrement();
    if (count.intValue() == 0) {
      fileMap.remove(filePath);
    }
    metricsFileMap.put(filePath, tuple);
    if (!queue.contains(filePath)) {
      queue.add(filePath);
    }
    LOG.debug("Removing: file: {}, map size: {}", tuple, fileMap.size());
  }
  
  @Override
  public void endWindow()
  {
    super.endWindow();
    sendFileStatus();
    sendFileDetails();
  }

  private void sendFileDetails()
  {
    List<Map<String, Object>> toBeEmitted = Lists.newArrayList();
    Object[] array = queue.toArray();
    for (int i=(array.length-1); i>=0; --i) {
      ExtendedModuleFileMetaData meta = metricsFileMap.get((String) array[i]);
      Map<String, Object> m = Maps.newHashMap();
      m.put(FILEDETAILS_METRICS_NAME, meta.isDirectory() ? meta.getFileName() + "/" : meta.getFileName());
      m.put(FILEDETAILS_METRICS_OUT_PATH, meta.getOutputRelativePath());
      m.put(FILEDETAILS_METRICS_SIZE, (double) (meta.getFileLength() / 1024.0 / 1024.0));
      //TODO: Uncomment following to support compression, encryption related metrics
//      
//      m.put(FILEDETAILS_METRICS_COMPR_RATIO,
//            meta.isDirectory() ? "-" : meta.getOutputFileSize() == 0 ? "1.0" : NumberFormat.getNumberInstance().format(1.0 * meta.getOutputFileSize() / meta.getFileLength()));
//      m.put(FILEDETAILS_METRICS_COMPR_TIME, 
//            meta.getCompressionTime() == 0 ? "-" : NumberFormat.getNumberInstance().format(meta.getCompressionTime() / 1000.0 / 1000.0 / 1000.0));
//      m.put(FILEDETAILS_METRICS_ENC_TIME, 
//            meta.getEncryptionTime() == 0 ? "-" : NumberFormat.getNumberInstance().format(meta.getEncryptionTime() / 1000.0 / 1000.0 / 1000.0));
//      
      m.put(FILEDETAILS_METRICS_OVERALL_TIME, 
            (meta.getCompletionTime() == 0) ? "-" : NumberFormat.getNumberInstance().format((meta.getCompletionTime() - meta.getDiscoverTime()) / 1000.0));
      
      String status = "Unknown";
      if (meta.getCompletionStatus() == TrackerEventType.DISCOVERED) {
        status = "Discovered";
      }
      else if (meta.getCompletionStatus() == TrackerEventType.SUCCESSFUL_FILE) {
        status = "Successful";
      }
      else if (meta.getCompletionStatus() == TrackerEventType.SKIPPED_FILE) {
        status = "Skipped";
      }
      else if (meta.getCompletionStatus() == TrackerEventType.FAILED_FILE) {
        status = "Failed";
      }
      m.put(FILEDETAILS_METRICS_STATUS, status);
      toBeEmitted.add(m);
    }
    fileDetailsMetrics.emit(toBeEmitted);
  }

  private void sendFileStatus()
  {
    if (!metricsFileMap.isEmpty()) {
      String[] status = new String[]{"Remaining Items", "Successful Items", "Skipped Items", "Failed Items"};
      int count[] = new int[status.length];
      long sizes[] = new long[status.length];
      
      for (ExtendedModuleFileMetaData meta : metricsFileMap.values()) {
        if (meta.getCompletionStatus() == TrackerEventType.DISCOVERED) {
          count[0]++;
          sizes[0] += meta.getFileLength();
        }
        else if (meta.getCompletionStatus() == TrackerEventType.SUCCESSFUL_FILE) {
          count[1]++;
          sizes[1] += meta.getFileLength();
        }
        else if (meta.getCompletionStatus() == TrackerEventType.SKIPPED_FILE) {
          count[2]++;
          sizes[2] += meta.getFileLength();
        }
        else if (meta.getCompletionStatus() == TrackerEventType.FAILED_FILE) {
          count[3]++;
          sizes[3] += meta.getFileLength();
        }
      }
      
      List<Map<String, Object>> toBeEmitted = Lists.newArrayList();
      for (int i=0; i<4; i++) {
        Map<String, Object> m = Maps.newHashMap();
        m.put(STATUS_METRICS_STATUS, status[i]);
        m.put(STATUS_METRICS_COUNT, count[i]);
        m.put(STATUS_METRICS_TOTAL_SIZE, (double)(sizes[i] / 1024.0 / 1024.0));
        toBeEmitted.add(m);
      }
      LOG.debug("Emitting to snapServer: {}" + toBeEmitted);
      statusMetrics.emit(toBeEmitted);
      remainingFileCounts = (count[0] > remainingFileCounts) ? (count[0] - remainingFileCounts) : 0;
    }
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

  
  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }

  public final transient DefaultInputPort<TrackerEvent> fileSplitterTracker = new DefaultInputPort<TrackerEvent>() {
    @Override
    public void process(TrackerEvent event)
    {
      trackerEventOutPort.emit(event);
    }
  };
  
  public final transient DefaultInputPort<TrackerEvent> mergerTracker = new DefaultInputPort<TrackerEvent>() {
    @Override
    public void process(TrackerEvent event)
    {
      trackerEventOutPort.emit(event);
    }
  };  
  
  private static final Logger LOG = LoggerFactory.getLogger(Tracker.class);
}
