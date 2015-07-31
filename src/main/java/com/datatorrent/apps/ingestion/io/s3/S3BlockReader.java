package com.datatorrent.apps.ingestion.io.s3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.Application.Scheme;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.malhar.lib.io.block.ReaderContext;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * <p>S3BlockReader class.</p>
 *
 * @since 1.0.0
 */
public class S3BlockReader extends BlockReader
{
  private String s3bucket;

  private transient String s3bucketUri;

  private String userKey;

  private String passKey;

  public S3BlockReader()
  {
    super(Scheme.S3N);
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    Preconditions.checkArgument(uri != null || (s3bucket != null && userKey != null && passKey != null), "missing uri or s3 bucket/authentication information.");

    if (s3bucket != null && userKey != null && passKey != null) {
      s3bucketUri = Application.Scheme.S3N + "://" + s3bucket;
      return FileSystem.newInstance(URI.create(Application.Scheme.S3N + "://" + userKey + ":" + passKey + "@" + s3bucket + "/"), configuration);
    }
    s3bucketUri = Application.Scheme.S3N + "://" + extractBucket(uri);
    return FileSystem.newInstance(URI.create(uri), configuration);
  }

  @VisibleForTesting
  protected String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  @Override
  protected FSDataInputStream setupStream(FileBlockMetadata block) throws IOException
  {
    FSDataInputStream ins = ((NativeS3FileSystem) fs).open(new Path(s3bucketUri + block.getFilePath()));
    ins.seek(block.getOffset());
    return ins;
  }

  /**
   * @return the s3bucket
   */
  public String getS3bucket()
  {
    return s3bucket;
  }

  /**
   * @param s3bucket
   *          the s3bucket to set
   */
  public void setS3bucket(String s3bucket)
  {
    this.s3bucket = s3bucket;
  }

  /**
   * @return the userKey
   */
  public String getUserKey()
  {
    return userKey;
  }

  /**
   * @param userKey
   *          the userKey to set
   */
  public void setUserKey(String userKey)
  {
    this.userKey = userKey;
  }

  /**
   * @return the passKey
   */
  public String getPassKey()
  {
    return passKey;
  }

  /**
   * @param passKey
   *          the passKey to set
   */
  public void setPassKey(String passKey)
  {
    this.passKey = passKey;
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
        bytesToRead = (int) (blockMetadata.getLength() - offset);
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
}
