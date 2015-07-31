/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;

/**
 * Operator to add entries into compaction metadata file.
 * There will be one entry per input file.
 *
 * @since 1.0.0
 */
public class MetaFileWriter extends AbstractFileOutputOperator<String>
{
  /**
   * Suffix used for compaction meta file.
   */
  public static final String META_FILE_SUFFIX = ".CompactionMeta";
  
  public static final String META_FILE_HEADER = "dir start_partition    start_offset   end_partition      end_offset filename\n";
  
  /**
   * Name of the compactionBundle. 
   * Assumption: compactionBundleName is unique name within output directory.
   * User is responsible for providing unique name within output directory.
   */
  private String compactionBundleName;
  
  /**
   * Name of the meta file
   */
  transient private String metaFileName;
  
  /**
   * Initialize meta file name.
   * @see com.datatorrent.lib.io.fs.AbstractFileOutputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    compactionBundleName = context.getValue(Context.DAGContext.APPLICATION_NAME);
    metaFileName = compactionBundleName + META_FILE_SUFFIX;
    
    //Add fileheader to the file before writing any entries
    MutableLong currentOffset = endOffsets.get(metaFileName);
    if(currentOffset == null || currentOffset.longValue() == 0L){
      processTuple(META_FILE_HEADER);
    } 
  }
  
  @Override
  protected String getFileName(String tuple)
  {
    return metaFileName;
  }

  /**
   * Convert index entry into byte array
   */
  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }
  
  /**
   * @return the compactionBundleName
   */
  public String getCompactionBundleName()
  {
    return compactionBundleName;
  }
  
  /**
   * @param compactionBundleName the compactionBundleName to set
   */
  public void setCompactionBundleName(String compactionBundleName)
  {
    this.compactionBundleName = compactionBundleName;
  }
  
  /**
   * @return the metaFileName
   */
  public String getMetaFileName()
  {
    return metaFileName;
  }
  
  
  /**
   * @param metaFileName the metaFileName to set
   */
  public void setMetaFileName(String metaFileName)
  {
    this.metaFileName = metaFileName;
  }
}
