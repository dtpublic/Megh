package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String>
{
  
  private String outputFileNamePrefix = "messageData";
  private String outputFileNameFormat = "%s.%d";
  private String messageSeperator ="\n";
  
  /**
   * 
   */
  public FileOutputOperator()
  {
    maxLength = 67108864L;
  }

  @Override
  protected String getFileName(String tuple)
  {
    int operatorId = context.getId();
    return String.format(outputFileNameFormat, outputFileNamePrefix, operatorId);
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    StringBuffer sb = new StringBuffer(tuple);
    sb.append(messageSeperator);
    return String.valueOf(sb).getBytes();
  }
  
  /**
   * @return the messageSeperator
   */
  public String getMessageSeperator()
  {
    return messageSeperator;
  }
  
  /**
   * @param messageSeperator the messageSeperator to set
   */
  public void setMessageSeperator(String messageSeperator)
  {
    this.messageSeperator = messageSeperator;
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
