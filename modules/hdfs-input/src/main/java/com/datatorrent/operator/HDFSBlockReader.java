/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.operator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.utils.URIUtils;

/**
 * HDFSBlockReader extends {@link BlockReader} to do HDFS specific
 * initialization
 */
public class HDFSBlockReader extends BlockReader
{
  protected String uri;

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(URI.create(uri), configuration);
  }

  /**
   * Sets the uri
   *
   * @param uri
   */
  public void setUri(String uri)
  {
    this.uri = URIUtils.convertSchemeToLowerCase(uri);
  }

  public String getUri()
  {
    return uri;
  }

}
