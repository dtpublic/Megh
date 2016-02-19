package com.datatorrent.app.operators;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class BytesNonAppendFileOutputOperator extends BytesFileOutputOperator
{
  private static String APPEND_TMP_FILE = "_APPENDING";

  @Override
  protected FSDataOutputStream openStream(Path filepath, boolean append) throws IOException
  {
    if (append) {
      //Since underlying filesystems do not support append, we have to achieve that behavior by writing to a tmp
      //file and then re-writing the contents back to stream opened in create mode for filepath.
      Path appendTmpFile = new Path(filepath + APPEND_TMP_FILE);
      rename(filepath, appendTmpFile);
      FSDataInputStream fsIn = fs.open(appendTmpFile);
      FSDataOutputStream fsOut = fs.create(filepath);
      IOUtils.copy(fsIn, fsOut);
      flush(fsOut);
      fs.delete(appendTmpFile);
      return fsOut;
    }
    return super.openStream(filepath, false);
  }


  @Override
  protected void rename(Path source, Path destination) throws IOException
  {
    fs.rename(source, destination);
  }
}
