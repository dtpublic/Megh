/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

import org.apache.hadoop.fs.Path;

/**
 * Defining new type of exception for missing block. Currently, methods catching
 * this exception assumes that block is missing because of explicit deletion by
 * Ingestion App (for completed files)
 *
 */
public class BlockNotFoundException extends Exception
{

  private static final long serialVersionUID = -7409415466834194798L;

  Path blockPath;

  /**
   * @param blockPath
   */
  public BlockNotFoundException(Path blockPath)
  {
    super();
    this.blockPath = blockPath;
  }

  /**
   * @return the blockPath
   */
  public Path getBlockPath()
  {
    return blockPath;
  }

}
