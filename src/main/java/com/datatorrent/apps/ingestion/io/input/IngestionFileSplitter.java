package com.datatorrent.apps.ingestion.io.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

public class IngestionFileSplitter extends FileSplitter
{
  @VisibleForTesting
  protected transient Path[] filePathArray;
  public transient static String currentDir;
  private boolean scanNowFlag;
  private static HashMap<String, String> fileMap;

  public IngestionFileSplitter()
  {
    super();
    scanner = new RecursiveDirectoryScanner();
  }

  @Override
  public void setup(OperatorContext context)
  {
    String fullDir = directory;
    String[] dirs = fullDir.split(",");
    filePathArray = new Path[dirs.length];
    int i = 0;
    for (String str : dirs) {
      filePathArray[i++] = new Path(str);
      if (i == 0) {
        directory = str;
      }
    }
    super.setup(context);
    
    fileMap = new HashMap<String, String>();
  }

  @Override
  protected void scanDirectory()
  {
    if (scanNowFlag) {
      scanForNewFiles();
      scanNowFlag = false;
    } else if (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      scanForNewFiles();
      lastScanMillis = System.currentTimeMillis();
    }
  }

  private void scanForNewFiles()
  {
    Set<Path> newPaths = ((RecursiveDirectoryScanner) scanner).scan(fs, filePathArray, processedFiles);

    for (Path newPath : newPaths) {
      String newPathString = newPath.toString();
      pendingFiles.add(newPathString);
      processedFiles.add(newPathString);
      localProcessedFileCount.increment();
    }
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

    public boolean isDirectory()
    {
      return isDirectory;
    }

    public void setDirectory(boolean directory)
    {
      this.isDirectory = directory;
    }

    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    private boolean isDirectory;
    private String relativePath;

  }

  public static class RecursiveDirectoryScanner extends AbstractFileInputOperator.DirectoryScanner
  {
    /**
     * 
     */
    private static final long serialVersionUID = 6957453841555811744L;

    private String ignoreFilePatternRegexp;
    private transient Pattern ignoreRegex = null;
    private boolean recursiveScan = false;

    public String getIgnoreFilePatternRegexp()
    {
      return ignoreFilePatternRegexp;
    }

    public void setIgnoreFilePatternRegexp(String ignoreFilePatternRegexp)
    {
      this.ignoreFilePatternRegexp = ignoreFilePatternRegexp;
      this.ignoreRegex = null;
    }

    protected Pattern getIgnoreRegex()
    {
      if (this.ignoreRegex == null && this.ignoreFilePatternRegexp != null)
        this.ignoreRegex = Pattern.compile(this.ignoreFilePatternRegexp);
      return this.ignoreRegex;
    }

    public LinkedHashSet<Path> scan(FileSystem fs, Path[] filePathArray, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      for (Path path : filePathArray) {
        try {
          FileStatus fileStatus = fs.getFileStatus(path);
          if (fileStatus.isDirectory()) {
            currentDir = fileStatus.getPath().toString();
          }else{
            currentDir = fileStatus.getPath().getParent().toString();
          }
        }catch (FileNotFoundException fnf){
          LOG.error("File/Directory does not exist: {}", path,fnf);
        }catch (IOException e) {
          LOG.error("Unable to get current directory for path: {}", path);
          throw new RuntimeException("Failure in setting current directory.", e);
        }
        LOG.debug("Setting current direcotry: {}", currentDir);
        pathSet.addAll(scan(fs, path, consumedFiles));
      }
      return pathSet;
    }

    @Override
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with filePatternRegexp={}, ignoreFilePatternRegexp={} recursiveScan={}", filePath, this.getRegex(), this.ignoreFilePatternRegexp, this.recursiveScan);

        Path[] pathList = null;
        try {
          pathList = getRecursivePaths(fs, filePath.toString());
        } catch (URISyntaxException e) {
        }

        for (Path path : pathList) {
          String filePathStr = path.toString();
          LOG.debug("filePathStr is: {}", filePathStr);

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (ignoredFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          } else {
            // don't look at it again
            ignoredFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    protected boolean acceptFile(String filePathStr)
    {

      boolean accepted = super.acceptFile(filePathStr);
      if (!accepted) {
        return false;
      }

      Pattern ignoreRegex = this.getIgnoreRegex();
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(filePathStr);
        // If matched against ignored Regex then do not accept the file.
        if (matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    private Path[] getRecursivePaths(FileSystem fs, String basePath) throws IOException, URISyntaxException
    {
      List<Path> result = new ArrayList<Path>();
      Path path = new Path(basePath);
      FileStatus[] listStatus = null;
      if(fs.isDirectory(path)){
        listStatus = fs.globStatus(new Path(basePath + "/*"));  
      }else{
        listStatus = fs.globStatus(new Path(basePath)); // Single file
      }
      
      for (FileStatus fstat : listStatus) {
        readSubDirectory(fstat, basePath, fs, result);
      }
      return (Path[]) result.toArray(new Path[result.size()]);
    }

    private void readSubDirectory(FileStatus fileStatus, String basePath, FileSystem fs, List<Path> paths) throws IOException, URISyntaxException
   {
      if (fs.isDirectory(fileStatus.getPath()) && !recursiveScan) {
        return;
      }

      LOG.debug("Adding : {}",fileStatus.getPath());
      paths.add(fileStatus.getPath());
      String subPath = fileStatus.getPath().toString();
      if (!fileMap.containsKey(subPath)) {
        fileMap.put(subPath, currentDir);
      }
      FileStatus[] listStatus = fs.globStatus(new Path(subPath + "/*"));
      for (FileStatus fst : listStatus) {
        readSubDirectory(fst, subPath, fs, paths);
      }
    }

    /**
     * @return the recursiveScan
     */
    public boolean isRecursiveScan()
    {
      return recursiveScan;
    }

    /**
     * @param recursiveScan
     *          the recursiveScan to set
     */
    public void setRecursiveScan(boolean recursiveScan)
    {
      this.recursiveScan = recursiveScan;
    }

  }

  @Override
  protected IngestionFileMetaData buildFileMetadata(String fPath) throws IOException
  {
    currentFile = fPath;
    Path path = new Path(fPath);

    IngestionFileMetaData fileMetadata = new IngestionFileMetaData(currentFile);
    fileMetadata.setFileName(path.getName());

    FileStatus status = fs.getFileStatus(path);
    if (status.isDirectory()) {
      fileMetadata.setFileLength(0);
      fileMetadata.setDirectory(true);
      fileMetadata.setNumberOfBlocks(0);
      fileMetadata.setRelativePath(fPath.substring(fileMap.get(fPath).length() + 1));
    } else {
      int noOfBlocks = (int) ((status.getLen() / blockSize) + (((status.getLen() % blockSize) == 0) ? 0 : 1));
      if (fileMetadata.getDataOffset() >= status.getLen()) {
        noOfBlocks = 0;
      }
      fileMetadata.setFileLength(status.getLen());
      fileMetadata.setNumberOfBlocks(noOfBlocks);
      if(fileMap.get(fPath) == null){ // Direct filename is given as input.
        fileMetadata.setRelativePath(path.getName());
      }else{
        fileMetadata.setRelativePath(fPath.toString().substring(fileMap.get(fPath).length() + 1));  
      }
    }
    LOG.debug("Setting relateive path as {}  for file {}", fileMap.get(fPath), fPath);

    populateBlockIds(fileMetadata);
    return fileMetadata;
  }

  public boolean isScanNowFlag()
  {
    return scanNowFlag;
  }

  public void setScanNowFlag(boolean scanNowFlag)
  {
    this.scanNowFlag = scanNowFlag;
  }

  private static final Logger LOG = LoggerFactory.getLogger(IngestionFileSplitter.class);
}
