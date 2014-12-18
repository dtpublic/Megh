package com.datatorrent.apps.ingestion.io.input;

import java.io.IOException;

import com.datatorrent.lib.io.fs.FileSplitter.BlockMetadata;

public class FTPBlockReader extends BlockReader
{

  @Override
  protected com.datatorrent.lib.io.fs.AbstractBlockReader.Entity readEntity(BlockMetadata blockMetadata, long blockOffset) throws IOException
  {
    super.entity.clear();
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
}
