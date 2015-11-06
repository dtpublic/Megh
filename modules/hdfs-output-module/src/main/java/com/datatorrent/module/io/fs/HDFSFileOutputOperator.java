package com.datatorrent.module.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @param separator Separator between the tuples
   */
  public void setTupleSeparator(String separator)
  {
    this.tupleSeparator = separator;
    this.tupleSeparatorBytes = separator.getBytes();
  }

  private static Logger LOG = LoggerFactory.getLogger(HDFSFileOutputOperator.class);
}
