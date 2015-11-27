/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.operator;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.ModuleFileSplitter;

/**
 * HDFSFileSplitter extends {@link ModuleFileSplitter} to: <br/>
 * 1. Ignore hadoop temporary files i.e. files with extension _COPYING_ <br/>
 * 2. Ignore files with unsupported characters in filepath
 */
public class HDFSFileSplitter extends ModuleFileSplitter
{
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    Scanner scanner = (Scanner)getScanner();
    if (scanner.getIgnoreFilePatternRegularExp() == null) {
      scanner.setIgnoreFilePatternRegularExp(".*._COPYING_");
    }
  }

  public static class HDFSScanner extends Scanner
  {
    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (containsUnsupportedCharacters(filePathStr)) {
        return false;
      }
      return accepted;
    }

    private boolean containsUnsupportedCharacters(String filePathStr)
    {
      return new Path(filePathStr).toUri().getPath().contains(":");
    }
  }
}
