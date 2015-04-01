package com.datatorrent.apps.ingestion.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.Application;
import com.google.common.base.Preconditions;

public class S3BlockReader extends BlockReader
{
  private String uri;

  private String filePath;

  private String s3bucket;

  private String userKey;

  private String passKey;

  public S3BlockReader()
  {
    super();
    userKey = "";
    passKey = "";
    scheme = Application.Schemes.S3;
  }

  @Override
  protected FileSystem getFSInstance()
  {
    Preconditions.checkArgument(uri != null || s3bucket != null, "missing uri or bucket");

    FileSystem s3fs = new S3FileSystem();
    try {
      if (uri != null) {
        s3fs.initialize(URI.create(uri), configuration);
      } else {
        String s3Uri = "s3n://" + userKey + ":" + passKey + "@" + s3bucket + "/" + filePath;
        LOG.debug("s3 uri {}", s3Uri);
        s3fs.initialize(URI.create(s3Uri), configuration);
      }
    } catch (IOException e) {
      try {
        s3fs.close();
      } catch (IOException e1) {
        LOG.error("Unable to close s3 file sytem.", e1);
      }
      throw new RuntimeException("Unable to initialize s3 file system.", e);
    }
    return s3fs;
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3BlockReader.class);

  /**
   * @return the uri
   */
  public String getUri()
  {
    return uri;
  }

  /**
   * @param uri
   *          the uri to set
   */
  public void setUri(String uri)
  {
    this.uri = uri;
  }

  /**
   * @return the filePath
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * @param filePath
   *          the filePath to set
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
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

}
