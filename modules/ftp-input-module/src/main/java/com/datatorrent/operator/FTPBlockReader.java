package com.datatorrent.operator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;
import com.datatorrent.lib.io.input.BlockReader;
import com.google.common.base.Preconditions;

/**
 * <p>FTPBlockReader class.</p>
 *
 * @since 1.0.0
 */
public class FTPBlockReader extends BlockReader
{
  public static final String SCHEME = "ftp";

  private String inputUri;

  public FTPBlockReader()
  {
    this.readerContext = new FTPBlockReaderContext();
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    Preconditions.checkArgument(inputUri != null, "missing uri");
    DTFTPFileSystem fileSystem = new DTFTPFileSystem();
    URI inputURI = URI.create(inputUri);
    String uriWithoutPath = inputUri.replaceAll(inputURI.getPath(), "");
    fileSystem.initialize(URI.create(uriWithoutPath), configuration);
    return fileSystem;
  }

  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    LOG.debug("FTPBlockReader:initReaderFor: {}", block.getFilePath());
    if(fs == null){
      fs = getFSInstance();
    }
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
      stream.readFully(record, 0, bytesToRead);
      entity.setUsedBytes(bytesToRead);
      entity.setRecord(record);

      return entity;
    }
  }

  public String getInputUri() {
    return inputUri;
  }

  public void setInputUri(String inputUri) {
    this.inputUri = inputUri;
  }
}
