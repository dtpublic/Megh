/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.module;

import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.operator.HDFSBlockReader;
import com.datatorrent.operator.HDFSFileSplitter;

/**
 * HDFSInputModule is used to read files from HDFS. <br/>
 * Module emits FileMetadata, BlockMetadata and the block bytes.
 */
public class HDFSInputModule extends FSInputModule
{
  public ModuleFileSplitter getFileSplitter()
  {
    return new HDFSFileSplitter();
  }

  public BlockReader getBlockReader()
  {
    HDFSBlockReader br = new HDFSBlockReader();
    br.setUri(getFiles());
    return br;
  }
}
