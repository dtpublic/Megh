/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.common.IngestionUtils;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * FileOutput operator to write byte[]
 *
 * @since 1.0.0
 */
public class BytesFileOutputOperator extends AbstractFileOutputOperator<byte[]>
{
  /**
   * Prefix used for generating output file name
   */
  private String outputFileNamePrefix = "messageData";
  
  private String outputFileExtension = null;

  /**
   * File Name format for output files 
   */
  private String outputFileNameFormat = "%s.%d";
  
  /**
   * Separator character will be added after every message 
   */
  private String messageSeparator ="";
  
  /**
   * Default file size for rolling file
   */
  private static final long MB_64 = 64*1024*1024L;
  
  @AutoMetric
  private long bytesPerSec;
  
  private long byteCount;
  private double windowTimeSec; 
  
  /**
   * 
   */
  public BytesFileOutputOperator()
  {
    maxLength = MB_64;
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }



  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesPerSec = 0;
    byteCount = 0;
  }

  /**
   * Derives output file name for given tuple
   * @param tuple : Tuple 
   */
  @Override
  protected String getFileName(byte[] tuple)
  {
    int operatorId = context.getId();
    String fileName = String.format(outputFileNameFormat, outputFileNamePrefix, operatorId);
    if (outputFileExtension != null) {
      fileName = fileName + "." + outputFileExtension;
    }
    return fileName;
  }

  /**
   * Convert tuple to byte[] and add separator character
   */
  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();
    
    try {
      bytesOutStream.write(tuple);
      bytesOutStream.write(messageSeparator.getBytes());
      byteCount += bytesOutStream.size();
      return bytesOutStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally{
      try {
        bytesOutStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    bytesPerSec = (long) (byteCount / windowTimeSec);
  }

  /**
   * @return the messageSeparator
   */
  public String getMessageSeparator()
  {
    return messageSeparator;
  }
  
  /**
   * @param messageSeparator the messageSeparator to set
   */
  public void setMessageSeparator(String messageSeparator)
  {
    this.messageSeparator = messageSeparator;
  }
  
  
  /**
   * @return the outputFileNamePrefix
   */
  public String getOutputFileNamePrefix()
  {
    return outputFileNamePrefix;
  }
  
  /**
   * @param outputFileNamePrefix the outputFileNamePrefix to set
   */
  public void setOutputFileNamePrefix(String outputFileNamePrefix)
  {
    this.outputFileNamePrefix = outputFileNamePrefix;
  }
  
  /**
   * @return the outputFileNameFormat
   */
  public String getOutputFileNameFormat()
  {
    return outputFileNameFormat;
  }
  
  /**
   * @param outputFileNameFormat the outputFileNameFormat to set
   */
  public void setOutputFileNameFormat(String outputFileNameFormat)
  {
    this.outputFileNameFormat = outputFileNameFormat;
  }

  /**
   * @return output file extension
   */
  public String getOutputFileExtension()
  {
    return outputFileExtension;
  }

  /**
   * @param outputFileExtension outputFile extension to set
   */
  public void setOutputFileExtension(String outputFileExtension)
  {
    this.outputFileExtension = outputFileExtension;
  }
  
  @Override
  public void setFilePath(@Nonnull String dir)
  {
	this.filePath = IngestionUtils.convertSchemeToLowerCase(dir);
  }

}
