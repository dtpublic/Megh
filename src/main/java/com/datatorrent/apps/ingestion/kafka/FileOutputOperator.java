package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String>
{

  
  public static final String OUTPUT_FILENAME = "messageData";
  private String messageSeperator ="\n";

  @Override
  protected String getFileName(String tuple)
  {
    int operatorId = context.getId();
    return OUTPUT_FILENAME + "." + operatorId;
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

}
