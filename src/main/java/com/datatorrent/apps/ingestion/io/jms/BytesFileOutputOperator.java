/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;

/**
 * FileOutput operator to write byte[]
 */
public class BytesFileOutputOperator extends AbstractFileOutputOperator<byte[]>
{
  /**
   * Prefix used for generating output file name
   */
  private String outputFileNamePrefix = "messageData";
  
  /**
   * File Name format for output files 
   */
  private String outputFileNameFormat = "%s.%d";
  
  /**
   * Separator character will be added after every message 
   */
  private String messageSeparator ="\n";
  
  /**
   * Default file size for rolling file
   */
  private static final long MB_64 = 64*1024*1024L;
  
  /**
   * 
   */
  public BytesFileOutputOperator()
  {
    maxLength = MB_64;
  }

  /**
   * Derives output file name for given tuple
   * @param tuple : Tuple 
   */
  @Override
  protected String getFileName(byte[] tuple)
  {
    int operatorId = context.getId();
    return String.format(outputFileNameFormat, outputFileNamePrefix, operatorId);
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

}
