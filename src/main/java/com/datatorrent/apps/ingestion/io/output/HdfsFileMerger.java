/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.google.common.annotations.VisibleForTesting;

/**
 * This merges the various small block files to the main file
 * 
 * @author Yogi/Sandeep
 */

public class HdfsFileMerger extends BaseOperator
{
  protected transient FileSystem appFS, outputFS;
  @NotNull
  protected String filePath;
  protected String blocksPath;
  private String skippedListFile;

  private boolean deleteSubFiles;
  private boolean overwriteOutputFile;
  private boolean dfsAppendSupport;

  private long defaultBlockSize;
  private List<String> skippedFiles;
  long skippedListFileLength;

  private static final String HDFS_STR = "hdfs";
  private static final String PART_FILE_EXTENTION = ".part";
  protected static final String STATS_DIR = "ingestionStats";
  protected static final String SKIPPED_FILE = "skippedFiles";
  protected static final String NEW_LINE_CHARACTER = "\n";

  private static final int COPY_BUFFER_SIZE = 1024;
  private static final int BLOCK_SIZE = 1024 * 64; // 64 KB, default buffer size on HDFS
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileMerger.class);

  public HdfsFileMerger()
  {
    deleteSubFiles = true;
    skippedFiles = new ArrayList<String>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    blocksPath = context.getValue(DAG.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS;
    skippedListFile = context.getValue(DAG.APPLICATION_PATH) + File.separator + STATS_DIR + File.separator + SKIPPED_FILE;
    try {
      outputFS = FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
      appFS = FileSystem.newInstance((new Path(blocksPath)).toUri(), new Configuration());
      dfsAppendSupport = outputFS.getConf().getBoolean("dfs.support.append", true);
      defaultBlockSize = outputFS.getDefaultBlockSize(new Path(filePath));
      recoverSkippedListFile();
    } catch (IOException ex) {
      LOG.error("Exception in FileMerger setup.", ex);
      throw new RuntimeException(ex);
    }
  }

  private void recoverSkippedListFile()
  {
    try {
      Path skippedListFilePath = new Path(skippedListFile);
      if (appFS.exists(skippedListFilePath) && appFS.getFileStatus(skippedListFilePath).getLen() > skippedListFileLength) {
        Path partFilePath = new Path(skippedListFile + PART_FILE_EXTENTION);
        FSDataInputStream inputStream = appFS.open(skippedListFilePath);
        FSDataOutputStream fsOutput = appFS.create(partFilePath, true);

        try {
          byte[] buffer = new byte[COPY_BUFFER_SIZE];
          while (inputStream.getPos() < skippedListFileLength) {
            long remainingBytes = skippedListFileLength - inputStream.getPos();
            int bytesToWrite = remainingBytes < COPY_BUFFER_SIZE ? (int) remainingBytes : COPY_BUFFER_SIZE;
            inputStream.read(buffer);
            fsOutput.write(buffer, 0, bytesToWrite);
          }
          fsOutput.flush();
        } catch (IOException e) {
          throw e;
        } finally {
          fsOutput.close();
          inputStream.close();
          fsOutput = null;
          inputStream = null;
        }
        FileContext fileContext = FileContext.getFileContext(appFS.getUri());
        LOG.debug("temp file path {}, rolling file path {}", partFilePath.toString(), skippedListFileLength);
        fileContext.rename(partFilePath, skippedListFilePath, Options.Rename.OVERWRITE);
      }
    } catch (IOException e) {
      LOG.error("Error while recovering skipped file list.", e);
    }

  }

  public final transient DefaultInputPort<FileSplitter.FileMetadata> processedFileInput = new DefaultInputPort<FileSplitter.FileMetadata>() {
    @Override
    public void process(FileSplitter.FileMetadata fileMetadata)
    {
      mergeFiles(fileMetadata);
    }
  };

  @Override
  public void teardown()
  {
    try {
      if (appFS != null) {
        appFS.close();
      }
      if (outputFS != null) {
        outputFS.close();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    appFS = null;
    outputFS = null;

  }

  @Override
  public void endWindow()
  {
    FSDataOutputStream outStream = null;
    if (skippedFiles.size() > 0) {
      try {
        outStream = getStatsOutputStream();
        for (String fileName : skippedFiles) {
          outStream.writeBytes(fileName + NEW_LINE_CHARACTER);
        }
        outStream.flush();

        skippedListFileLength = appFS.getFileStatus(new Path(skippedListFile)).getLen();
        skippedFiles.clear();
      } catch (IOException e) {
        LOG.error("Error while writting skipped file list to HDFS", e);
      } finally {
        if (outStream != null) {
          try {
            outStream.close();
            outStream = null;
          } catch (IOException e) {
            LOG.error("Error closing file handle for skipped list file {} ", skippedListFile);
          }
        }
      }
    }
  }

  private FSDataOutputStream getStatsOutputStream() throws IOException
  {
    Path skippedListFilePath = new Path(skippedListFile);
    if (appFS.exists(skippedListFilePath)) {
      return appFS.append(skippedListFilePath);
    }
    return appFS.create(skippedListFilePath);
  }

  private void mergeFiles(FileSplitter.FileMetadata fileMetadata)
  {
    IngestionFileMetaData iFileMetadata = null;
    if (fileMetadata instanceof IngestionFileMetaData) {
      iFileMetadata = (IngestionFileMetaData) fileMetadata;
    }
    LOG.debug(" Relative path: {}", iFileMetadata.getRelativePath());
    String absolutePath = filePath + Path.SEPARATOR + iFileMetadata.getRelativePath();
    Path outputFilePath = new Path(absolutePath);
    try {
      if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
        LOG.info("Output file {} already exits and overwrite flag is off.", outputFilePath);
        LOG.info("Skipping writing output file.");
        skippedFiles.add(absolutePath);
        return;
      }
    } catch (IOException e) {
      LOG.error("Unable to check existance of outputfile: " + absolutePath);
      throw new RuntimeException("Exception during checking of existance of outputfile.", e);
    }

    int numBlocks = iFileMetadata.getNumberOfBlocks();
    Path outputPartFilePath = new Path(absolutePath + PART_FILE_EXTENTION);

    try {
      if (iFileMetadata.isDirectory()) {
        if (!outputFS.exists(outputFilePath)) {
          outputFS.mkdirs(outputFilePath);
        }
        return;
      }
    } catch (IOException e) {
      LOG.error("Unable to create directory {}", outputFilePath);
    }

    if (!allBlocksPresent(iFileMetadata)) {
      LOG.debug("At least one block found missing. Attempting auto-recovery.");
      recover(iFileMetadata);
      return;
    }

    if (numBlocks == 0) { // 0 size file, touch the file
      if (iFileMetadata.getFileLength() == 0) {
        FSDataOutputStream outputStream = null;
        try {
          outputStream = outputFS.create(outputPartFilePath);
          moveFile(outputPartFilePath, outputFilePath);
        } catch (IOException e) {
          LOG.error("Unable to create zero size file {}.", outputFilePath, e);
          throw new RuntimeException("Unable to create zero size file.");
        } finally {
          if (outputStream != null) {
            try {
              outputStream.close();
            } catch (IOException e) {
              LOG.error("Unable to close output stream while creating zero size file.");
            }
            outputStream = null;
          }
        }
      }
      return;
    }

    long[] blocksArray = iFileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksPath, Long.toString(blocksArray[0]));// The first block.

    // Apply merging optimizations only if output FS is same as block FS
    if (appFS.getUri().equals(outputFS.getUri())) {
      // File == 1 block only.
      if (numBlocks == 1) {
        moveFile(firstBlock, outputFilePath);
        return;
      }

      boolean sameSize = matchBlockSize(fileMetadata, defaultBlockSize);
      LOG.debug("Fast merge possible: {}", sameSize && dfsAppendSupport && HDFS_STR.equalsIgnoreCase(outputFS.getScheme()));
      if (sameSize && dfsAppendSupport && HDFS_STR.equalsIgnoreCase(outputFS.getScheme())) {
        // Stitch and append the file.
        // Conditions:
        // 1. dfs.support.append should be true
        // 2. intermediate blocks size = HDFS block size
        // 3. Output should be on HDFS
        LOG.info("Attempting fast merge.");
        try {
          stitchAndAppend(iFileMetadata);
          return;
        } catch (Exception e) {
          LOG.error("Fast merge failed. {}", e);
          throw new RuntimeException("Unable to merge file on HDFS: " + fileMetadata.getFileName());
        }
      }

    }
    LOG.info("Merging by reading and writing blocks serially..");
    mergeBlocksSerially(iFileMetadata);
  }

  private void mergeBlocksSerially(FileSplitter.FileMetadata fmd)
  {
    IngestionFileMetaData fileMetadata = null;
    if (fmd instanceof IngestionFileMetaData) {
      fileMetadata = (IngestionFileMetaData) fmd;
    }

    String fileName = fileMetadata.getRelativePath();
    Path path = new Path(filePath, fileName);
    Path partFilePath = new Path(filePath, fileName + PART_FILE_EXTENTION);
    Path[] blockFiles = new Path[fileMetadata.getNumberOfBlocks()];
    int index = 0;
    for (long blockId : fileMetadata.getBlockIds()) {
      blockFiles[index] = new Path(blocksPath, Long.toString(blockId));
      index++;
    }
    FSDataOutputStream outputStream = null;
    try {
      outputStream = outputFS.create(partFilePath);
      byte[] inputBytes = new byte[BLOCK_SIZE];
      int inputBytesRead;
      for (Path blockPath : blockFiles) {
        if (!appFS.exists(blockPath)) {
          LOG.error("Missing block {} of file {}", blockPath, fileName);
          throw new RuntimeException("Missing block: " + blockPath);
        }
        DataInputStream is = new DataInputStream(appFS.open(blockPath));
        while ((inputBytesRead = is.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
        is.close();
      }
      if (deleteSubFiles) {
        for (Path blockPath : blockFiles) {
          appFS.delete(blockPath, false);
        }
      }
    } catch (IOException ex) {
      try {
        if (outputFS.exists(partFilePath)) {
          outputFS.delete(partFilePath, false);
        }
      } catch (IOException e) {
      }
      throw new RuntimeException(ex);
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to close output stream.", e);
      }
    }
    moveFile(partFilePath, path);
  }

  private void stitchAndAppend(IngestionFileMetaData fileMetadata) throws IOException
  {
    Path outputFilePath = new Path(filePath, fileMetadata.getRelativePath());

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksPath, Long.toString(blocksArray[0]));
    Path[] blockFiles = new Path[numBlocks - 1]; // Leave the first block

    for (int index = 1; index < numBlocks; index++) {
      blockFiles[index - 1] = new Path(blocksPath, Long.toString(blocksArray[index]));
    }

    try {
      outputFS.concat(firstBlock, blockFiles);
      moveFile(firstBlock, outputFilePath);
    } catch (IOException e) {
      LOG.error("Exception in merging file {} with HDFS concat.", outputFilePath, e);
      throw new RuntimeException(e);
    }
  }

  private boolean matchBlockSize(FileSplitter.FileMetadata fileMetadata, long defaultBlockSize)
  {
    try {
      Path firstBlockPath = new Path(blocksPath + Path.SEPARATOR + fileMetadata.getBlockIds()[0]);
      if (!appFS.exists(firstBlockPath)) {
        LOG.error("Missing block {} of file {}", firstBlockPath, fileMetadata.getFileName());
        throw new RuntimeException("Missing block: " + firstBlockPath);
      }

      FileStatus status = appFS.getFileStatus(firstBlockPath);
      if (status.getLen() % defaultBlockSize == 0) { // Equal or multiple of HDFS block size
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to verify if reader block size and HDFS block size is same.");
    }
    return false;
  }

  private void moveFile(Path src, Path dst)
  {
    // FTP rename fails if fullpaths (with scheme, authority) are passed.
    // TODO:Check if this works for other fileSystems.
    if ("ftp".equals(src.toUri().getScheme())) {
      src = Path.getPathWithoutSchemeAndAuthority(src);
      dst = Path.getPathWithoutSchemeAndAuthority(dst);
    }
	// Move the file to right destination.
    boolean moveSuccessful;
    try {
      if (!outputFS.exists(dst.getParent())) {
        outputFS.mkdirs(dst.getParent());
      }
      if (outputFS.exists(dst)) {
        outputFS.delete(dst, false);
      }
      moveSuccessful = outputFS.rename(src, dst);
    } catch (IOException e) {
      LOG.error("File move failed from {} to {} ", src, dst, e);
      throw new RuntimeException("Failed to move file to destination folder.", e);
    }
    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      LOG.error("Move file {} to {} failed.", src, dst);
      throw new RuntimeException("Moving file to output folder failed.");
    }
  }

  @VisibleForTesting
  protected boolean recover(IngestionFileMetaData iFileMetadata)
  {
    try {
      Path firstBlockPath = new Path(blocksPath + Path.SEPARATOR + iFileMetadata.getBlockIds()[0]);
      String absolutePath = filePath + Path.SEPARATOR + iFileMetadata.getRelativePath();
      Path outputFilePath = new Path(absolutePath);
      if (appFS.exists(firstBlockPath)) {
        FileStatus status = appFS.getFileStatus(firstBlockPath);
        if (status.getLen() == iFileMetadata.getFileLength()) {
          moveFile(firstBlockPath, outputFilePath);
          return true;
        } else {
          LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
          return false;
        }
      } else {
        if (outputFS.exists(outputFilePath)) {
          LOG.debug("Output file already present at the destination, nothing to recover.");
          return true;
        }
        LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
        return false;
      }
    } catch (IOException e) {
      LOG.error("Error in recovering.", e);
      throw new RuntimeException("Unable to recover.");
    }
  }

  @VisibleForTesting
  protected boolean allBlocksPresent(IngestionFileMetaData iFileMetadata)
  {

    long[] blockIds = iFileMetadata.getBlockIds();
    if (null == blockIds) {
      return true;
    }
    for (long blockId : blockIds) {
      try {
        boolean blockExists = appFS.exists(new Path(blocksPath + Path.SEPARATOR + blockId));
        if (!blockExists) {
          return false;
        }
      } catch (IOException e) {
        LOG.error("Unable to check existance of block for file : {} ", iFileMetadata.getRelativePath(), e);
        throw new RuntimeException("Unable to check existance of block.", e);
      }
    }
    return true;
  }

  public List<String> getSkippedFilesList()
  {
    return skippedFiles;
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  public boolean isDeleteSubFiles()
  {
    return deleteSubFiles;
  }

  public void setDeleteSubFiles(boolean deleteSubFiles)
  {
    this.deleteSubFiles = deleteSubFiles;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }
}
