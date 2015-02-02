package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.block.FSSliceReader;

public class BlockReader extends FSSliceReader
{

  protected String directory; // Same as FileSpiltter directory.

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(new Path(directory).toUri(), configuration);
  }

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockReader.class);

}
