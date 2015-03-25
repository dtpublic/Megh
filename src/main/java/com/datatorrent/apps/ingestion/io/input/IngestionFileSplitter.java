package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
import com.datatorrent.lib.io.fs.FileSplitter;

public class IngestionFileSplitter extends FileSplitter
{
  public static final String IDEMPOTENCY_RECOVERY = "idempotency";

  public IngestionFileSplitter()
  {
    super();
    scanner = new Scanner();
    ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath("");
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (idempotentStorageManager instanceof FSIdempotentStorageManager) {
      String recoveryPath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + IDEMPOTENCY_RECOVERY;
      ((FSIdempotentStorageManager) idempotentStorageManager).setRecoveryPath(recoveryPath);
    }
    fileCounters.setCounter(PropertyCounters.THRESHOLD, new MutableLong());
    super.setup(context);
  }

  @Override
  public void endWindow()
  {
    fileCounters.getCounter(PropertyCounters.THRESHOLD).setValue(blocksThreshold);
    super.endWindow();
  }

  public static class IngestionFileMetaData extends FileSplitter.FileMetadata
  {
    protected IngestionFileMetaData()
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
    private transient Pattern ignoreRegex = null;

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

    if (fileInfo.getDirectoryPath() == null) {  // Direct filename is given as input.
      fileMetadata.setRelativePath(status.getPath().getName());
    }
    else {
      fileMetadata.setRelativePath(fileInfo.getRelativeFilePath());
    }

    LOG.debug("Setting relative path as {}  for file {}", fileMetadata.getRelativePath(), filePathStr);

    return fileMetadata;
  }

  public static enum PropertyCounters
  {
    THRESHOLD
  }

  private static final Logger LOG = LoggerFactory.getLogger(IngestionFileSplitter.class);
}
