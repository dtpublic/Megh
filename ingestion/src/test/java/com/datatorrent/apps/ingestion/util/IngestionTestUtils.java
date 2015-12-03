package com.datatorrent.apps.ingestion.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IngestionTestUtils
{
  private static void traverseDirectory(StringBuilder sb, FileSystem fs, Path path) throws IOException
  {
    sb.append(path.getName() + ";");

    FileStatus s = fs.getFileStatus(path);
    if (s.isDirectory()) {
      FileStatus[] s1 = fs.listStatus(path);
      Arrays.sort(s1);
      for (FileStatus s2 : s1) {
        traverseDirectory(sb, fs, s2.getPath());
      }
    }
  }

  public static boolean compareInputInsideOutput(Path inpDir, Path outDir) throws IOException
  {
    String inputName = inpDir.getName();
    outDir = new Path(outDir, inputName);
    
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    if (!fs.exists(outDir)) {
      return false;
    }

    StringBuilder sbInp = new StringBuilder();
    StringBuilder sbOut = new StringBuilder();
    traverseDirectory(sbInp, fs, inpDir);
    traverseDirectory(sbOut, fs, outDir);
    System.out.println(sbInp.toString());
    System.out.println(sbOut.toString());
    if (!sbInp.toString().equals(sbOut.toString())) {
      return false;
    }
    return true;
  }
}
