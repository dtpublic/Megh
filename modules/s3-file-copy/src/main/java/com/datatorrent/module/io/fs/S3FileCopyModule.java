/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module.io.fs;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.lib.io.output.IngestionFileMerger;
import com.datatorrent.module.FSOutputModule;
import com.datatorrent.module.io.fs.Operator.S3FileMerger;

/**
 * S3FileCopyModule extends from FSOutputModule and writes files into Amazon S3 bucket.&nbsp;
 * <p>
 * Properties:<br>
 * <b>accesskeyId </b>: Access key Id of Amazon S3. <br>
 * <b>secretAccessId</b>: Secret Access key Id of Amazon S3. <br>
 * <b>bucketName</b>: Name of S3 bucket, where the data copies. <br>
 * </p>
 * @displayName S3 Output Module
 * @category Files
 * @tags Output Module
 *
 */
public class S3FileCopyModule extends FSOutputModule
{
  @NotNull
  private String accesskeyId;
  @NotNull
  private String secretAccessId;
  @NotNull
  private String bucketName;

  /**
   * Returns the S3FileMerger
   * @return
   */
  @Override
  public IngestionFileMerger getFileMerger()
  {
    return new S3FileMerger();
  }

  /**
   * Constructs the file path using Amazon credentials
   * @return the file path
   */
  @Override
  protected String constructFilePath()
  {
    StringBuffer sb = new StringBuffer("s3n://");
    sb.append(accesskeyId);
    sb.append(":");
    sb.append(secretAccessId);
    sb.append("@");
    sb.append(bucketName);
    sb.append("/");
    sb.append(directory);
    return sb.toString();
  }

  /**
   * Return the Access Key Id.
   * @return the accesskey
   */
  public String getAccesskeyId()
  {
    return accesskeyId;
  }

  /**
   * Sets the accessKeyId with the given id
   * @param accesskeyId given access key Id
   */
  public void setAccesskeyId(String accesskeyId)
  {
    this.accesskeyId = accesskeyId;
  }

  /**
   * Return the Secret Access Id.
   * @return the secret access id.
   */
  public String getSecretAccessId()
  {
    return secretAccessId;
  }

  /**
   * Sets the Secret Access Id.
   * @param secretAccessId given secret access Id.
   */
  public void setSecretAccessId(String secretAccessId)
  {
    this.secretAccessId = secretAccessId;
  }

  /**
   * Returns the bucket name
   * @return the bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Sets the bucket name
   * @param bucketName given bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
