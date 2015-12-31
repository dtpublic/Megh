/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.operator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.DTS3FileSystem;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.ReaderContext;
import com.datatorrent.lib.io.input.BlockReader;

/**
 * S3BlockReader extends from BlockReader and serves the functionality of reads and
 * parse blocks metadata.
 */
public class S3BlockReader extends BlockReader
{
  public static final String SCHEME = "DTS3";

  private transient String s3bucketUri;
  private String inputURI;

  public S3BlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    DTS3FileSystem s3System = new DTS3FileSystem();
    s3bucketUri = SCHEME + "://" + extractBucket(inputURI);
    s3System.initialize(URI.create(inputURI), configuration);
    return s3System;
  }

  @VisibleForTesting
  protected String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    FSDataInputStream ins = fs.open(new Path(s3bucketUri + block.getFilePath()));
    ins.seek(block.getOffset());
    return ins;
  }

  /**
   * BlockReadeContext for reading S3 Blocks.<br/>
   * This should use read API without offset.
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int)(blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      int bytesRead = 0;
      while (bytesRead < bytesToRead) {
        bytesRead += stream.read(record, bytesRead, bytesToRead - bytesRead);
      }
      entity.setUsedBytes(bytesRead);
      entity.setRecord(record);
      return entity;
    }
  }

  /**
   * Returns the inputURI
   * @return
   */
  public String getInputURI()
  {
    return inputURI;
  }

  /**
   * Sets the inputURI which is in the form of s3://accessKey:secretKey@bucketName/
   * @param inputURI inputURI
   */
  public void setInputURI(String inputURI)
  {
    this.inputURI = inputURI;
  }
}
