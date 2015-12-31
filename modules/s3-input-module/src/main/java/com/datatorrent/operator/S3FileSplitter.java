/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.operator;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.DTS3FileSystem;
import com.datatorrent.lib.io.input.ModuleFileSplitter;

/**
 * S3FileSplitter extends from ModuleFileSplitter and serves the functionality
 * of splitting the S3 file and creates metadata describing the files and splits.
 */
public class S3FileSplitter extends ModuleFileSplitter
{
  public S3FileSplitter()
  {
    super();
    super.setScanner(new S3Scanner());
  }

  public static class S3Scanner extends Scanner
  {
    private String inputURI;
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      DTS3FileSystem s3System = new DTS3FileSystem();
      s3System.initialize(URI.create(inputURI), new Configuration());
      return s3System;
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
}
