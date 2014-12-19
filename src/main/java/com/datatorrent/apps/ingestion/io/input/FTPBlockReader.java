package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.io.DTFTPFileSystem;
import com.datatorrent.lib.io.fs.FileSplitter.BlockMetadata;

public class FTPBlockReader extends BlockReader
{

  @Override
  public void setup(OperatorContext context)
  {

    LOG.debug("FTPBlockReader:setup");
    super.setup(context);
    // Overwriting fs
    try {
      fs = new DTFTPFileSystem();
      String ftpUri = "ftp://yogi:data512@192.168.1.82:21";
      fs.initialize(URI.create(ftpUri), configuration);
//          DTFTPFileSystem.newInstance(new Path(directory).toUri(), configuration); 
//          FileSystem.newInstance(new Path(directory).toUri(), configuration);
    } catch (Exception e) {
      throw new RuntimeException("Unable to create filesystem instance for " + directory, e);
    }

  }

  @Override
  protected com.datatorrent.lib.io.fs.AbstractBlockReader.Entity readEntity(BlockMetadata blockMetadata, long blockOffset) throws IOException
  {
    super.entity.clear();
    LOG.debug("FTPBlockReader:readEntity: {}:{}", blockMetadata.getFilePath(),blockOffset);
    int bytesToRead = length;
    if (blockOffset + length >= blockMetadata.getLength()) {
      bytesToRead = (int) (blockMetadata.getLength() - blockOffset);
    }
    byte[] record = new byte[bytesToRead];
    inputStream.read(record, 0, bytesToRead);
    entity.usedBytes = bytesToRead;
    entity.record = record;

    return entity;
  }
  
  @Override
  protected void initReaderFor(BlockMetadata blockMetadata) throws IOException
  {
    LOG.debug("FTPBlockReader:initReaderFor: {}", blockMetadata.getFilePath());
    inputStream = ((DTFTPFileSystem)fs).open(new Path(blockMetadata.getFilePath()),4096,blockMetadata.getOffset());
  }
  private static final Logger LOG = LoggerFactory.getLogger(FTPBlockReader.class);

}
