package com.datatorrent.apps.ingestion.io;

import java.io.IOException;

import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.io.fs.FixedBytesBlockReader;

public class FixedBytesBlockMessageReader extends FixedBytesBlockReader
{
  protected transient FileSplitter.BlockMetadata blockMetadata;
  
  @Override
  protected void initReaderFor(FileSplitter.BlockMetadata blockMetadata) throws IOException
  {
    super.initReaderFor(blockMetadata);
    this.blockMetadata =  blockMetadata;
  }

  @Override
  protected BlockData convertToRecord(byte[] bytes)
  {
    return new BlockData(bytes,blockMetadata.getBlockId());
  }
  
}
