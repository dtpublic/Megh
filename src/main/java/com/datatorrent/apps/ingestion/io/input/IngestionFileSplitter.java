package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.apps.ingestion.io.ftp.DTFTPFileSystem;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.malhar.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
import com.datatorrent.malhar.lib.io.fs.FileSplitter;

public class IngestionFileSplitter extends FileSplitter
{
  public static final String IDEMPOTENCY_RECOVERY = "idempotency";
  private boolean fastMergeEnabled = false;

  private transient String oneTimeCopyComplete;
  private transient Path oneTimeCopyCompletePath;
  private transient FileSystem appFS;
  private String compressionExtension;

  public IngestionFileSplitter()
  {
    super();
    scanner = new Scanner();
    ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath("");
    blocksThreshold = 1;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (idempotentStorageManager instanceof FSIdempotentStorageManager) {
      String recoveryPath = IDEMPOTENCY_RECOVERY;
      ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath(recoveryPath);
    }
    fileCounters.setCounter(PropertyCounters.THRESHOLD, new MutableLong());

    fastMergeEnabled = fastMergeEnabled && (blockSize == null);
    super.setup(context);

    try {
      appFS = getAppFS();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    oneTimeCopyComplete = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + Application.ONE_TIME_COPY_DONE_FILE;
    oneTimeCopyCompletePath = new Path(oneTimeCopyComplete);

    // override blockSize calculated in setup() to default HDFS block size to enable fast merge on HDFS
    if (fastMergeEnabled) {
      blockSize = hdfsBlockSize(context.getValue(DAGContext.APPLICATION_PATH));
    }
  }

  private FileSystem getAppFS() throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (appFS != null) {
      try {
        appFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close application file system.", e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    fileCounters.getCounter(PropertyCounters.THRESHOLD).setValue(blocksThreshold);
    super.endWindow();

    if (((Scanner) scanner).isOneTimeCopy() && ((Scanner) scanner).isFirstScanComplete() && blockMetadataIterator == null) {
      try {
        checkCompletion();
      } catch (IOException e) {
        throw new RuntimeException("Unable to check shutdown signal file.", e);
      }
    }
  }

  private void checkCompletion() throws IOException
  {
    if (appFS.exists(oneTimeCopyCompletePath)) {
      LOG.info("One time copy completed. Sending shutdown signal.");
      throw new ShutdownException();
    }
  }

  private long hdfsBlockSize(String path)
  {
    return appFS.getDefaultBlockSize(new Path(path));
  }

  public static class IngestionFileMetaData extends FileSplitter.FileMetadata
  {
    public IngestionFileMetaData()
    {
      super();
    }

    public IngestionFileMetaData(String currentFile)
    {
      super(currentFile);
    }

    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    private String relativePath;

  }

  public static class Scanner extends TimeBasedDirectoryScanner
  {

    private String ignoreFilePatternRegularExp;
    private transient Pattern ignoreRegex;
    private boolean oneTimeCopy;
    private boolean firstScanComplete;

    @Override
    public void setup(OperatorContext context)
    {
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }
      super.setup(context);
    }

    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegularExp)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegularExp;
      this.ignoreRegex = null;
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (!accepted) {
        return false;
      }
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(filePathStr);
        // If matched against ignored Regex then do not accept the file.
        if (matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected void scanComplete()
    {
      super.scanComplete();
      if (oneTimeCopy) {
        running = false;
      }
      firstScanComplete = true;
    }

    @Override protected Path createPathObject(String aFile)
    {
      String pathURI = files.iterator().next();
      URI inputURI = URI.create(pathURI);

      if (inputURI.getScheme().equalsIgnoreCase(Application.Scheme.FTP.toString())) {
        return new Path(StringUtils.stripStart(new Path(aFile).toUri().getPath(), "/"));
      }
      else {
        return super.createPathObject(aFile);
      }
    }

    @Override protected FileSystem getFSInstance() throws IOException
    {
      String pathURI = files.iterator().next();
      URI inputURI = URI.create(pathURI);

      if (inputURI.getScheme().equalsIgnoreCase(Application.Scheme.FTP.toString())) {
        DTFTPFileSystem fileSystem = new DTFTPFileSystem();
        String uriWithoutPath = pathURI.replaceAll(inputURI.getPath(), "");
        fileSystem.initialize(URI.create(uriWithoutPath), new Configuration());
        return fileSystem;
      }
      else {
        return super.getFSInstance();
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

    /**
     * @return the firstScanComplete
     */
    public boolean isFirstScanComplete()
    {
      return firstScanComplete;
    }

    /**
     * @param firstScanComplete
     *          the firstScanComplete to set
     */
    public void setFirstScanComplete(boolean firstScanComplete)
    {
      this.firstScanComplete = firstScanComplete;
    }

  }

  @Override
  protected IngestionFileMetaData buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    String filePathStr = fileInfo.getFilePath();
    LOG.debug("file {}", filePathStr);
    IngestionFileMetaData fileMetadata = new IngestionFileMetaData(filePathStr);
    Path path = new Path(filePathStr);

    fileMetadata.setFileName(path.getName());

    FileStatus status = fs.getFileStatus(path);
    fileMetadata.setDirectory(status.isDirectory());
    fileMetadata.setFileLength(status.getLen());

    if (!status.isDirectory()) {
      int noOfBlocks = (int) ((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
      if (fileMetadata.getDataOffset() >= status.getLen()) {
        noOfBlocks = 0;
      }
      fileMetadata.setNumberOfBlocks(noOfBlocks);
      populateBlockIds(fileMetadata);
    }

    if (fileInfo.getDirectoryPath() == null) { // Direct filename is given as input.
      fileMetadata.setRelativePath(status.getPath().getName());
    } else {
      fileMetadata.setRelativePath(fileInfo.getRelativeFilePath());
    }

    if (compressionExtension != null && !fileMetadata.isDirectory()) {
      String extension = "." + compressionExtension;
      fileMetadata.setRelativePath(fileMetadata.getRelativePath() + extension);
    }

    LOG.debug("Setting relative path as {}  for file {}", fileMetadata.getRelativePath(), filePathStr);

    return fileMetadata;
  }

  @Override
  public void setBlocksThreshold(int threshold)
  {
    LOG.debug("blocks threshold changed to {}", threshold);
    super.setBlocksThreshold(threshold);
  }

  public static enum PropertyCounters {
    THRESHOLD
  }

  /**
   * @return the fastMergeEnabled
   */
  public boolean isFastMergeEnabled()
  {
    return fastMergeEnabled;
  }

  /**
   * @param fastMergeEnabled
   *          the fastMergeEnabled to set
   */
  public void setFastMergeEnabled(boolean fastMergeEnabled)
  {
    this.fastMergeEnabled = fastMergeEnabled;
  }

  public void setcompressionExtension(String compressionExtension)
  {
    this.compressionExtension = compressionExtension;
  }

  private static final Logger LOG = LoggerFactory.getLogger(IngestionFileSplitter.class);
}
