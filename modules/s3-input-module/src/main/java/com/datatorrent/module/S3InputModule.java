/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.operator.S3BlockReader;
import com.datatorrent.operator.S3FileSplitter;

/**
 * S3InputModule which extends from FSInputModule and serves the functionality of reading files
 * from S3 and emits FileMetadata, BlockMetadata and the block bytes.&nbsp;
 * <p>
 * <br>
 * Properties:<br>
 * <b>accesskeyId</b>: Access key Id of Amazon S3. <br>
 * <b>secretAccessId</b>: Secret Access key Id of Amazon S3. <br>
 * <b>bucketName</b>: Name of S3 bucket, where data resides. <br>
 *
 */
public class S3InputModule extends FSInputModule
{
  private String accesskeyId;
  private String secretAccessId;
  private String bucketName;
  private transient String inputURI;

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    generateURIAndConcatToFiles();
    super.populateDAG(dag,configuration);
  }

  protected void generateURIAndConcatToFiles()
  {
    inputURI = S3BlockReader.SCHEME + "://" + accesskeyId + ":" + secretAccessId + "@" + bucketName + "/";
    String uriFiles = "";
    String[] inputFiles = getFiles().split(",");
    for (int i = 0; i < inputFiles.length; i++) {
      uriFiles += inputURI + inputFiles[i];
      if (i != inputFiles.length - 1) {
        uriFiles += ",";
      }
    }
    setFiles(uriFiles);
  }

  @Override
  public ModuleFileSplitter getFileSplitter()
  {
    S3FileSplitter fileSplitter = new S3FileSplitter();
    S3FileSplitter.S3Scanner scanner = (S3FileSplitter.S3Scanner)fileSplitter.getScanner();
    scanner.setInputURI(inputURI);
    return fileSplitter;
  }

  @Override
  public BlockReader getBlockReader()
  {
    S3BlockReader reader = new S3BlockReader();
    reader.setInputURI(inputURI);
    return reader;
  }

  /**
   * Return the access key id
   * @return accesskeyId
   */
  public String getAccesskeyId()
  {
    return accesskeyId;
  }

  /**
   * Sets the access key id
   * @param accesskeyId given accesskeyId
   */
  public void setAccesskeyId(String accesskeyId)
  {
    this.accesskeyId = accesskeyId;
  }

  /**
   * Return the secret access key Id.
   * @return secretAccessId
   */
  public String getSecretAccessId()
  {
    return secretAccessId;
  }

  /**
   * Sets the secret access key Id.
   * @param secretAccessId
   */
  public void setSecretAccessId(String secretAccessId)
  {
    this.secretAccessId = secretAccessId;
  }

  /**
   * Returns the name of the bucket
   * @return bucketName
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Sets the bucket name.
   * @param bucketName bucketName
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
