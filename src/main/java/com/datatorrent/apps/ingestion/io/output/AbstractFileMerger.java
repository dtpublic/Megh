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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.annotations.VisibleForTesting;

/**
 * This merges the various small block files to the main file
 * 
 */

public class AbstractFileMerger extends AbstractReconciler<FileMetadata, FileMetadata>
{
  protected transient FileSystem appFS, outputFS;

  @NotNull
  private String outputDir;
  private String blocksDir;
  private String skippedListFile;

  private boolean deleteSubFiles;
  private boolean overwriteOutputFile;

  private long defaultBlockSize;
  private List<String> skippedFiles;
  long skippedListFileLength;
  
  private static final String PART_FILE_EXTENTION = ".part";
  protected static final String STATS_DIR = "ingestionStats";
  protected static final String SKIPPED_FILE = "skippedFiles";
  
  private int bufferSize = 64 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileMerger.class);

  public AbstractFileMerger()
  {
    deleteSubFiles = true;
    skippedFiles = new ArrayList<String>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    blocksDir = context.getValue(DAG.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS;
    skippedListFile = context.getValue(DAG.APPLICATION_PATH) + File.separator + STATS_DIR + File.separator + SKIPPED_FILE;
    try {
      outputFS = getFSInstance(outputDir);
      appFS = getFSInstance(blocksDir);
      setDefaultBlockSize(outputFS.getDefaultBlockSize(new Path(outputDir)));
      recoverSkippedListFile();
    } catch (IOException ex) {
      LOG.error("Exception in FileMerger setup.", ex);
      throw new RuntimeException(ex);
    }
  }

  protected FileSystem getFSInstance(String dir) throws IOException
  {
    return FileSystem.newInstance((new Path(dir)).toUri(), new Configuration());
  }
  
  private void saveSkippedFiles()
  {
    if (skippedFiles.size() > 0) {
      try {
        FSDataOutputStream outStream = getStatsOutputStream();
        for (String fileName : skippedFiles) {
          outStream.writeBytes(fileName + System.getProperty("line.separator"));
        }
        outStream.flush();
        outStream.close();
        skippedListFileLength = appFS.getFileStatus(new Path(skippedListFile)).getLen();
        skippedFiles.clear();
      } catch (IOException e) {
        LOG.error("Unable to save skipped files.",e);
        throw new RuntimeException(e);
      }
    }
  }
  
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

  public void mergeFile(FileMetadata fmd)
  {
    IngestionFileMetaData fileMetadata = null;
    if (fmd instanceof IngestionFileMetaData) {
      fileMetadata = (IngestionFileMetaData) fmd;
    }


    String absolutePath = outputDir + Path.SEPARATOR + fileMetadata.getRelativePath();
    Path outputFilePath = new Path(absolutePath);
    LOG.info("Processing file: {}",fileMetadata.getRelativePath());
    
    if(fileMetadata.isDirectory()){
      createDir(outputFilePath);
      return;
    }

    try {
      if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
        LOG.info("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
        skippedFiles.add(absolutePath);
        saveSkippedFiles();
        return;
      }
    } catch (IOException e) {
      LOG.error("Unable to check existance of outputfile: " + absolutePath);
      throw new RuntimeException("Exception during checking of existance of outputfile.", e);
    }
    
    String fileName = fileMetadata.getRelativePath();
    Path path = new Path(outputDir, fileName);
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
          LOG.error("Missing block {} of file {}", blockPath, fileName);
          throw new RuntimeException("Missing block: " + blockPath);
        }
        DataInputStream is = new DataInputStream(appFS.open(blockPath));
        while ((inputBytesRead = is.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
        is.close();
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
    if (deleteSubFiles) {
      for (Path blockPath : blockFiles) {
        try {
          appFS.delete(blockPath, false);
        } catch (IOException e) {
          throw new RuntimeException("Unable to delete intermediate blocks.", e);
        }
      }
    }
  }

  private void createDir(Path outputFilePath)
  {
    try {
      if (!outputFS.exists(outputFilePath)) {
        outputFS.mkdirs(outputFilePath);
      }
    } catch (IOException e) {
      LOG.error("Unable to create directory {}", outputFilePath);
    }
  }

  @VisibleForTesting
  protected void moveFile(Path src, Path dst)
  {
    src = Path.getPathWithoutSchemeAndAuthority(src);
    dst = Path.getPathWithoutSchemeAndAuthority(dst);

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

  public List<String> getSkippedFilesList()
  {
    return skippedFiles;
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

  /**
   * @return the defaultBlockSize
   */
  public long getDefaultBlockSize()
  {
    return defaultBlockSize;
  }

  /**
   * @param defaultBlockSize
   *          the defaultBlockSize to set
   */
  public void setDefaultBlockSize(long defaultBlockSize)
  {
    this.defaultBlockSize = defaultBlockSize;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
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
  protected void processTuple(FileMetadata input)
  {
    enqueueForProcessing(input);
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
        LOG.debug("temp file path {}, rolling file path {}", partFilePath.toString(), skippedListFileLength);
        fileContext.rename(partFilePath, skippedListFilePath, Options.Rename.OVERWRITE);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Error while recovering skipped file list.", e);
    } catch (IOException e) {
      LOG.error("Error while recovering skipped file list.", e);
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
