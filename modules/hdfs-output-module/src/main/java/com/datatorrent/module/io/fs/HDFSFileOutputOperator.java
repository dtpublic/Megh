package com.datatorrent.module.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * This class is responsible for writing tuples to HDFS. All tuples are written
 * to the same file. Rolling over to the next file based on file size is
 * supported.
 *
 * @param <T>
 */
class HDFSFileOutputOperator<T> extends AbstractFileOutputOperator<T>
{

  /**
   * Name of the file to write the output. Directory for the output file should
   * be specified in the filePath
   */
  @NotNull
  private String fileName;

  /**
   * Separator between the tuples
   */
  @NotNull
  private String tupleSeparator;

  private byte[] tupleSeparatorBytes;

  @AutoMetric
  private long bytesPerSec;

  private long byteCount;
  private double windowTimeSec;
  
  private static final long  STREAM_EXPIRY_ACCESS_MILL = 24 * 60 * 60 * 1000L;
  private static final int  ROTATION_WINDOWS = 2 * 60 * 60 * 20;

  public HDFSFileOutputOperator()
  {

    setExpireStreamAfterAccessMillis(STREAM_EXPIRY_ACCESS_MILL);
    setMaxOpenFiles(1000);
    // Rotation window count = 20 hrs which is < expirestreamafteraccessmillis
    setRotationWindows(ROTATION_WINDOWS);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFileName(T tuple)
  {
    return fileName;
  }

  /**
   * {@inheritDoc}
   * 
   * @return byte[] representation of the given tuple. if input tuple is of type
   *         byte[] then it is returned as it is. for any other type toString()
   *         representation is used to generate byte[].
   */
  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    byte[] tupleData;
    if (tuple instanceof byte[]) {
      tupleData = (byte[])tuple;
    } else {
      tupleData = tuple.toString().getBytes();
    }

    ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

    try {
      bytesOutStream.write(tupleData);
      bytesOutStream.write(tupleSeparatorBytes);
      byteCount += bytesOutStream.size();
      return bytesOutStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bytesOutStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesPerSec = 0;
    byteCount = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    bytesPerSec = (long)(byteCount / windowTimeSec);
  }

  /**
   * @return File name for writing output. All tuples are written to the same
   *         file.
   * 
   */
  public String getFileName()
  {
    return fileName;
  }

  /**
   * @param fileName
   *          File name for writing output. All tuples are written to the same
   *          file.
   */
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }

  /**
   * @return Separator between the tuples
   */
  public String getTupleSeparator()
  {
    return tupleSeparator;
  }

  /**
   * @param separator
   *          Separator between the tuples
   */
  public void setTupleSeparator(String separator)
  {
    this.tupleSeparator = separator;
    this.tupleSeparatorBytes = separator.getBytes();
  }

}
