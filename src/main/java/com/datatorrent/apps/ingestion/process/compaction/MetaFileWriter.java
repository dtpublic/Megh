/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileCreator.IndexEntry;
import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;

/**
 * Operator to add entries into compaction metadata file.
 * There will be one entry per input file. 
 */
public class MetaFileWriter extends AbstractFileOutputOperator<IndexEntry>
{
  /**
   * Suffix used for compaction meta file.
   */
  private static final String META_FILE_SUFFIX = ".CompactionMeta";
  
  /**
   * Name of the compactionBundle. 
   * Assumption: compactionBundleName is unique name within output directory.
   * User is responsible for providing unique name within output directory.
   */
  @NotNull
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
    metaFileName = compactionBundleName + META_FILE_SUFFIX;
  }
  
  @Override
  protected String getFileName(IndexEntry tuple)
  {
    return metaFileName;
  }

  /**
   * Convert index entry into byte array
   */
  @Override
  protected byte[] getBytesForTuple(IndexEntry tuple)
  {
    return tuple.toString().getBytes();
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
