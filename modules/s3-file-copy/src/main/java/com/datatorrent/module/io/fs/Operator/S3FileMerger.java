/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module.io.fs.Operator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.DTS3FileSystem;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.output.BlockNotFoundException;
import com.datatorrent.lib.io.output.ExtendedModuleFileMetaData;
import com.datatorrent.lib.io.output.IngestionFileMerger;

/**
 * S3FileMerger which extends from IngestionFileMerger and serves the functionality of
 * writing s3 files.
 */
public class S3FileMerger extends IngestionFileMerger
{
  protected static final String HADOOP_TEMP_DIR = "hadoop.tmp.dir";
  protected static final String S3_TMP_DIR = "s3";

  protected transient Configuration conf;

  @Override
  public void setup(Context.OperatorContext context)
  {
    conf = new Configuration();
    if (conf.get(Constants.BUFFER_DIR) == null) {
      conf.set(Constants.BUFFER_DIR, conf.get(HADOOP_TEMP_DIR) + Path.SEPARATOR + S3_TMP_DIR + Path.SEPARATOR);
    }
    super.setup(context);
  }

  @Override
  protected FileSystem getOutputFSInstance() throws IOException
  {
    DTS3FileSystem s3System = new DTS3FileSystem();
    s3System.initialize(URI.create(filePath), conf);
    return s3System;
  }

  /**
   * S3 supports rename functionality. But, it effects the performance. Because, rename does the following steps:
   * 1) Downloads the object 2) Remove the object 3) Uploads the object with final name.
   * Instead of writing to temp file, it writes to final file. So, temp path is pointing to final path.
   * @param outFileMetadata File Meta info of output file
   * @return
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Override
  protected OutputStream writeTempOutputFile(ExtendedModuleFileMetaData outFileMetadata) throws IOException, BlockNotFoundException
  {
    tempOutFilePath = new Path(filePath, outFileMetadata.getOutputRelativePath());
    return super.writeTempOutputFile(outFileMetadata);
  }

  /**
   * WriteTempOutputFile writes to file path, so need to do any functionality in moveToFinalFile.
   * @param outFileMetadata
   * @throws IOException
   */
  @Override
  protected void moveToFinalFile(ExtendedModuleFileMetaData outFileMetadata) throws IOException
  {
  }
}
