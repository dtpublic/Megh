package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.io.DTFTPFileSystem;
import com.datatorrent.lib.io.block.BlockMetadata;

public class FTPBlockReader extends BlockReader
{

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    DTFTPFileSystem fs = new DTFTPFileSystem();
    fs.initialize(URI.create(directory), configuration);
    return fs;
  }

  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    LOG.debug("FTPBlockReader:initReaderFor: {}", block.getFilePath());
    return ((DTFTPFileSystem) fs).open(new Path(block.getFilePath()), 4096, block.getOffset());
  }

  private static final Logger LOG = LoggerFactory.getLogger(FTPBlockReader.class);

}
