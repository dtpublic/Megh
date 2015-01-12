package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.FixedBytesBlockReader;

public class BlockReader extends FixedBytesBlockReader
{

  protected String directory; // Same as FileSpiltter directory.

  @Override
  public void setup(OperatorContext context)
  {

    super.setup(context);
    // Overwriting fs
    try {
      fs = FileSystem.newInstance(new Path(directory).toUri(), configuration);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create filesystem instance for " + directory, e);
    }

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
