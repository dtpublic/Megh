/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.DataInputStream;
import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.annotations.VisibleForTesting;

/**
 * This merges various small block files to the main file
 * 
 */

public class FileMerger extends AbstractReconciler<FileMetadata, FileMetadata>
{
  protected transient FileSystem appFS, outputFS;

  @NotNull
  private String outputDir;
  private String blocksDir;
  private String skippedListFile;

  private boolean deleteBlocks;
  private boolean overwriteOutputFile;

  long skippedListFileLength;

  private static final String PART_FILE_EXTENTION = ".part";
  protected static final String STATS_DIR = "ingestionStats";
  protected static final String SKIPPED_FILE = "skippedFiles";
  protected static final String NEW_LINE_CHARACTER = "\n";

  private int bufferSize = 64 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);

  public FileMerger()
  {
    deleteBlocks = true;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
    skippedListFile = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + STATS_DIR + Path.SEPARATOR + SKIPPED_FILE;
    try {
      outputFS = getFSInstance(outputDir);
      appFS = getFSInstance(blocksDir);
      recoverSkippedListFile();
    } catch (IOException ex) {
      releaseResources();
      throw new RuntimeException("Exception in FileMerger setup.", ex);
    }
  }

  private void releaseResources()
  {
    boolean gotException = false;
    try {
      if (appFS != null) {
        appFS.close();
      }
    } catch (IOException e) {
      gotException = true;
    } finally {
      appFS = null;
    }

    try {
      if (outputFS != null) {
        outputFS.close();
      }
    } catch (IOException e) {
      gotException = true;
    } finally {
      outputFS = null;
    }
    if (gotException) {
      throw new RuntimeException("Exception while closing application file system.");
    }
  }

  protected FileSystem getFSInstance(String dir) throws IOException
  {
    return FileSystem.newInstance((new Path(dir)).toUri(), new Configuration());
  }

  private void saveSkippedFiles(String fileName) throws IOException
  {
    FSDataOutputStream outStream = null;
    try {
      outStream = getStatsOutputStream();
      outStream.writeBytes(fileName + NEW_LINE_CHARACTER);
      skippedListFileLength = appFS.getFileStatus(new Path(skippedListFile)).getLen();
    } catch (IOException e) {
      throw new RuntimeException("Exception: Unable to save skipped files list.", e);
    } finally {
      if (outStream != null) {
        outStream.flush();
        outStream.close();
      }
    }
  }

  @Override
  public void teardown()
  {
    releaseResources();
  }

  @VisibleForTesting
  protected void mergeFile(FileMetadata fmd)
  {
    IngestionFileMetaData fileMetadata = null;
    if (fmd instanceof IngestionFileMetaData) {
      fileMetadata = (IngestionFileMetaData) fmd;
    }

    if (null == fileMetadata) {
      LOG.error("Input tuple is not an instance of IngestionFileMetaData.");
      throw new RuntimeException("Input tuple is not an instance of IngestionFileMetaData.");
    }

    String absolutePath = outputDir + Path.SEPARATOR + fileMetadata.getRelativePath();
    Path outputFilePath = new Path(absolutePath);
    LOG.info("Processing file: {}", fileMetadata.getRelativePath());

    try {
      if (fileMetadata.isDirectory()) {
        createDir(outputFilePath);
        return;
      }

      if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
        LOG.debug("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
        saveSkippedFiles(absolutePath);
        return;
      }
    } catch (IOException e) {
      releaseResources();
      throw new RuntimeException("Exception: Unable to check existance of outputfile.", e);
    }

    String fileName = fileMetadata.getRelativePath();
    Path partFilePath = new Path(outputDir, fileName + PART_FILE_EXTENTION);
    Path[] blockFiles = new Path[fileMetadata.getNumberOfBlocks()];
    int index = 0;
    for (long blockId : fileMetadata.getBlockIds()) {
      blockFiles[index] = new Path(blocksDir, Long.toString(blockId));
      index++;
    }

    FSDataOutputStream outputStream = null;
    try {
      outputStream = outputFS.create(partFilePath);
      byte[] inputBytes = new byte[bufferSize];
      int inputBytesRead;
      for (Path blockPath : blockFiles) {
        if (!appFS.exists(blockPath)) {
          releaseResources();
          throw new RuntimeException("Exception: Missing block " + blockPath + "of file " + fileName);
        }
        DataInputStream is = new DataInputStream(appFS.open(blockPath));
        while ((inputBytesRead = is.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
        is.close();
      }
    } catch (IOException ex) {
      LOG.error("Unable to create part file {}", partFilePath, ex);
      try {
        if (outputFS.exists(partFilePath)) {
          outputFS.delete(partFilePath, false);
        }
      } catch (IOException e) {
        // Add to the list of failed files.
        releaseResources();
        throw new RuntimeException("Unable to delete part file " + partFilePath, e);
      }
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (IOException e) {
        releaseResources();
        LOG.error("Unable to release resources.", e);
      }
    }
    try {
      moveFile(partFilePath, outputFilePath);
    } catch (IOException e) {
      // TODO: Add to the list of failed files.
      releaseResources();
      throw new RuntimeException("File move failed from " + partFilePath + " to " + outputFilePath, e);
    }

    if (deleteBlocks) {
      for (Path blockPath : blockFiles) {
        try {
          appFS.delete(blockPath, false);
        } catch (IOException e) {
          releaseResources();
          throw new RuntimeException("Unable to delete intermediate blocks for file " + outputFilePath, e);
        }
      }
    }
    LOG.info("Completed processing file: {} ", fileMetadata.getRelativePath());
  }

  private void createDir(Path outputFilePath) throws IOException
  {
    if (!outputFS.exists(outputFilePath)) {
      outputFS.mkdirs(outputFilePath);
    }
  }

  @VisibleForTesting
  protected void moveFile(Path source, Path destination) throws IOException
  {
    Path src = Path.getPathWithoutSchemeAndAuthority(source);
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
    } else {
      releaseResources();
      throw new RuntimeException("Unable to move file from " + src + " to " + dst);
    }
  }

  public boolean isDeleteSubFiles()
  {
    return deleteBlocks;
  }

  public void setDeleteSubFiles(boolean deleteBlocks)
  {
    this.deleteBlocks = deleteBlocks;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }

  public String getOutputDir()
  {
    return outputDir;
  }

  public void setOutputDir(String outputDir)
  {
    this.outputDir = outputDir;
  }

  @Override
  protected void processTuple(FileMetadata fileMetadata)
  {
    enqueueForProcessing(fileMetadata);
  }

  @Override
  protected void processCommittedData(FileMetadata queueInput)
  {
    mergeFile(queueInput);
  }

  private void recoverSkippedListFile()
  {
    try {
      Path skippedListFilePath = new Path(skippedListFile);
      if (appFS.exists(skippedListFilePath) && appFS.getFileStatus(skippedListFilePath).getLen() != skippedListFileLength) {
        Path partFilePath = new Path(skippedListFile + PART_FILE_EXTENTION);
        FSDataInputStream inputStream = appFS.open(skippedListFilePath);
        FSDataOutputStream fsOutput = appFS.create(partFilePath, true);

        byte[] buffer = new byte[bufferSize];
        while (inputStream.getPos() < skippedListFileLength) {
          long remainingBytes = skippedListFileLength - inputStream.getPos();
          int bytesToWrite = remainingBytes < bufferSize ? (int) remainingBytes : bufferSize;
          inputStream.read(buffer);
          fsOutput.write(buffer, 0, bytesToWrite);
        }
        fsOutput.flush();
        fsOutput.close();
        inputStream.close();

        FileContext fileContext = FileContext.getFileContext(appFS.getUri());
        LOG.debug("temp file path {}, skipped file path {}", partFilePath.toString(), skippedListFileLength);
        fileContext.rename(partFilePath, skippedListFilePath, Options.Rename.OVERWRITE);
      }
    } catch (IOException e) {
      releaseResources();
      throw new RuntimeException("Error while recovering skipped file list.", e);
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

  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }

}
