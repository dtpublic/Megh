/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io;

import com.datatorrent.common.util.Slice;

/**
 * @author Yogi/Sandeep
 */
public class BlockData extends Slice
{

  private long blockId; 
  /**
   * 
   */
  private static final long serialVersionUID = -1188809119032631464L;

  private BlockData(){
    super(null);
  }

  public BlockData(byte[] array, long blockId)
  {
    super(array);
    this.blockId = blockId;
  }
  
  public long getBlockId(){
    return blockId;
  }
  
}
