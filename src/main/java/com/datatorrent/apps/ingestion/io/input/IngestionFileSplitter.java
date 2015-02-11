package com.datatorrent.apps.ingestion.io.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.google.common.collect.Sets;

public class IngestionFileSplitter extends FileSplitter
{

  private Path[] filePathArray;

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
    scanner = new RecursiveDirectoryScanner();
  }

  @Override
  protected void scanDirectory()
  {
    if (System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      Set<Path> newPaths = ((RecursiveDirectoryScanner) scanner).scan(fs, filePathArray, processedFiles);

      for (Path newPath : newPaths) {
        String newPathString = newPath.toString();
        pendingFiles.add(newPathString);
        processedFiles.add(newPathString);
        localProcessedFileCount.increment();
      }
      lastScanMillis = System.currentTimeMillis();
    }
  }

  public static class RecursiveDirectoryScanner extends AbstractFileInputOperator.DirectoryScanner
  {
    /**
     * 
     */
    private static final long serialVersionUID = 6957453841555811744L;

    public LinkedHashSet<Path> scan(FileSystem fs, Path[] filePathArray, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      for (Path path : filePathArray) {
        pathSet.addAll(scan(fs, path, consumedFiles));
      }
      return pathSet;
    }

    @Override
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with pattern {}", filePath, getRegex());

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

    public static Path[] getRecursivePaths(FileSystem fs, String basePath) throws IOException, URISyntaxException
    {
      List<Path> result = new ArrayList<Path>();
      FileStatus[] listStatus = fs.globStatus(new Path(basePath + "/*"));
      for (FileStatus fstat : listStatus) {
        readSubDirectory(fstat, basePath, fs, result);
      }
      return (Path[]) result.toArray(new Path[result.size()]);
    }

    private static void readSubDirectory(FileStatus fileStatus, String basePath, FileSystem fs, List<Path> paths) throws IOException, URISyntaxException
    {
      paths.add(fileStatus.getPath());
      String subPath = fileStatus.getPath().toString();
      FileStatus[] listStatus = fs.globStatus(new Path(subPath + "/*"));
      if (listStatus.length == 0) {
        paths.add(fileStatus.getPath());
      }
      for (FileStatus fst : listStatus) {
        readSubDirectory(fst, subPath, fs, paths);
      }
    }
  }

  @Override
  protected FileMetadata buildFileMetadata(String fPath) throws IOException
  {
    FileMetadata fileMetadata = super.buildFileMetadata(fPath);
    Path path = new Path(fPath);
    File f = new File(new Path(directory).toString());
    String baseDirUri = "file:" + f.getAbsolutePath();
    fileMetadata.setFileName(path.toString().substring(baseDirUri.length() + 1));
    LOG.debug("Adding filePath as : {}", fileMetadata.getFileName());

    FileStatus status = fs.getFileStatus(path);
    fileMetadata.setFileLength(status.isDirectory() ? -1 : status.getLen());
    return fileMetadata;
  }

  private static final Logger LOG = LoggerFactory.getLogger(IngestionFileSplitter.class);
}
