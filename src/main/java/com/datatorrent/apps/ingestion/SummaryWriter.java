/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;

/**
 * This operator writes summary information provided by Tracker to HDFS (appFS).
 */

public class SummaryWriter extends AbstractFileOutputOperator<TrackerEvent>
{

  private String successFilesListingFile = "successful_files.log";
  private String failedFilesListingFile = "failed_files.log";
  private String skippedFilesListingFile = "skipped_files.log";
  private String summaryListingFile = "summary_info.log";
  
  private String summaryLogsPath = "summary";
  
  /**
   * 
   */
  public SummaryWriter()
  {
    filePath = "";
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    filePath = context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + summaryLogsPath;
    super.setup(context);
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator#getFileName(java.lang.Object)
   */
  @Override
  protected String getFileName(TrackerEvent event)
  {
    switch (event.getType()) {
    case SUCCESSFUL_FILE:
      return successFilesListingFile;
    case FAILED_FILE:
      return failedFilesListingFile;
    case SKIPPED_FILE:
      return skippedFilesListingFile;
    case INFO:
      return summaryListingFile;
    default:
      throw new IllegalArgumentException("This Tracker event type is not supported");
    }
  }

  /* (non-Javadoc)
   * @see com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator#getBytesForTuple(java.lang.Object)
   */
  @Override
  protected byte[] getBytesForTuple(TrackerEvent event)
  {
    return (event.getDetails().getDescription() + SystemUtils.LINE_SEPARATOR).getBytes();
  }

  /**
   * @return the successFilesListingFile
   */
  public String getSuccessFilesListingFile()
  {
    return successFilesListingFile;
  }

  /**
   * @param successFilesListingFile the successFilesListingFile to set
   */
  public void setSuccessFilesListingFile(String successFilesListingFile)
  {
    this.successFilesListingFile = successFilesListingFile;
  }

  /**
   * @return the failedFilesListingFile
   */
  public String getFailedFilesListingFile()
  {
    return failedFilesListingFile;
  }

  /**
   * @param failedFilesListingFile the failedFilesListingFile to set
   */
  public void setFailedFilesListingFile(String failedFilesListingFile)
  {
    this.failedFilesListingFile = failedFilesListingFile;
  }

  /**
   * @return the skippedFilesListingFile
   */
  public String getSkippedFilesListingFile()
  {
    return skippedFilesListingFile;
  }

  /**
   * @param skippedFilesListingFile the skippedFilesListingFile to set
   */
  public void setSkippedFilesListingFile(String skippedFilesListingFile)
  {
    this.skippedFilesListingFile = skippedFilesListingFile;
  }

  /**
   * @return the summaryListingFile
   */
  public String getSummaryListingFile()
  {
    return summaryListingFile;
  }

  /**
   * @param summaryListingFile the summaryListingFile to set
   */
  public void setSummaryListingFile(String summaryListingFile)
  {
    this.summaryListingFile = summaryListingFile;
  }
  
  public String getSummaryLogsPath()
  {
    return summaryLogsPath;
  }
  
  public void setSummaryLogsPath(String summaryLogsPath)
  {
    this.summaryLogsPath = summaryLogsPath;
  }
}
