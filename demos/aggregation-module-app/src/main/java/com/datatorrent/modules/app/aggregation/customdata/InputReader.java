/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation.customdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class InputReader extends AbstractFileInputOperator<String>
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  private transient BufferedReader br = null;

  private Path path;

  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    path = curPath;
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
    path = null;
  }

  // return empty string
  @Override
  protected String readEntity() throws IOException
  {
    // try to read a line
    final String line = br.readLine();
    if (null != line) {    // common case
      return line;
    }

    return null;
  }

  @Override
  protected void emit(String tuple)
  {
    if (tuple != null) {
      output.emit(tuple);
    }
  }
}
