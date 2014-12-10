/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io;

/**
 * 
 */
public interface Message
{
  byte[] getBytes();

  long getBlockId();
}
