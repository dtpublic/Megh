/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.output;

import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;
import com.datatorrent.lib.io.output.TrackerEvent.TrackerEventType;

public class ExtendedModuleFileMetaData extends ModuleFileMetaData
{
  private long completionTime;
  private TrackerEventType completionStatus;
  private long compressionTime;
  private long outputFileSize;
  private long encryptionTime;

  public ExtendedModuleFileMetaData(String currentFile)
  {
    super(currentFile);
    completionStatus = TrackerEventType.DISCOVERED;
    compressionTime = 0;
    outputFileSize = 0;
    encryptionTime = 0;
  }

  public ExtendedModuleFileMetaData(ModuleFileMetaData moduleFileMetaData)
  {
    super(moduleFileMetaData.getFilePath());

    this.setFileName(moduleFileMetaData.getFileName());
    this.setNumberOfBlocks(moduleFileMetaData.getNumberOfBlocks());
    this.setDataOffset(moduleFileMetaData.getDataOffset());
    this.setFileLength(moduleFileMetaData.getFileLength());
    this.setDiscoverTime(moduleFileMetaData.getDiscoverTime());
    this.setBlockIds(moduleFileMetaData.getBlockIds());
    this.setDirectory(moduleFileMetaData.isDirectory());
    this.setRelativePath(moduleFileMetaData.getRelativePath());
    this.setOutputBlockMetaDataList(moduleFileMetaData.getOutputBlocksList());

    completionTime = 0;
    completionStatus = TrackerEventType.DISCOVERED;
    compressionTime = 0;
    outputFileSize = 0;
    encryptionTime = 0;
  }

  public long getCompletionTime()
  {
    return completionTime;
  }

  public void setCompletionTime(long completionTime)
  {
    this.completionTime = completionTime;
  }

  public TrackerEventType getCompletionStatus()
  {
    return completionStatus;
  }

  public void setCompletionStatus(TrackerEventType completionStatus)
  {
    this.completionStatus = completionStatus;
  }

  public long getCompressionTime()
  {
    return compressionTime;
  }

  public void setCompressionTime(long compressionTime)
  {
    this.compressionTime = compressionTime;
  }

  public long getOutputFileSize()
  {
    return outputFileSize;
  }

  public void setOutputFileSize(long outputFileSize)
  {
    this.outputFileSize = outputFileSize;
  }

  public long getEncryptionTime()
  {
    return encryptionTime;
  }

  public void setEncryptionTime(long encryptionTime)
  {
    this.encryptionTime = encryptionTime;
  }

}
