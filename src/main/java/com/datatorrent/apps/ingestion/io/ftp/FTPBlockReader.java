package com.datatorrent.apps.ingestion.io.ftp;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;

public class FTPBlockReader extends BlockReader
{

  /**
   * Sets FTP BlockReader Context
   */
  public FTPBlockReader()
  {
    super();
    this.readerContext = new FTPBlockReaderContext();
  }

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

  /**
   * BlockReadeContext for reading FTP Blocks.<br/>
   * This should use read API without offset.
   */
  private static class FTPBlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int) (blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      // If we use read call with offset then it will try to seek on FTP stream and throw exception
      // Hence use read call without offset
      // Offset handling is done using setRestartOffset in DTFTPFileSystem.java
      stream.read(record, 0, bytesToRead);
      entity.setUsedBytes(bytesToRead);
      entity.setRecord(record);

      return entity;
    }
  }
}
