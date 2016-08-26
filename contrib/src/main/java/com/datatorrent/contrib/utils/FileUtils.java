/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class holds utility methods related to files
 *
 */
public class FileUtils
{

  /**
   * This is a utility method which loads the contents of a file in hdfs into a string.
   * @param path of file in hdfs
   * @return contents of file as string
   */
  public static String readFromHDFS(String path) throws IOException
  {
    Path inputPath = new Path(path);
    StringWriter stringWriter = new StringWriter();
    try (FileSystem fs = FileSystem.get(new Configuration()); InputStream is = fs.open(inputPath)) {
      IOUtils.copy(is, stringWriter);
    }
    return stringWriter.toString();
  }
}
