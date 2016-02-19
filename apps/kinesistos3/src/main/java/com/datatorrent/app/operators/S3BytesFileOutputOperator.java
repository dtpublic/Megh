package com.datatorrent.app.operators;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;

public class S3BytesFileOutputOperator extends BytesNonAppendFileOutputOperator
{
  public static final String SCHEME = "s3n";
  private String accessKey;
  private String secretKey;
  private String bucketName;
  private String directoryName;

  private static final String TMP_EXTENSION = ".tmp";
  private static final Logger LOG = LoggerFactory.getLogger(S3BytesFileOutputOperator.class);
  @Override
  public void setup(Context.OperatorContext context)
  {
    generateURIAndConcatToFiles();
    endOffsets.clear();
    super.setup(context);
  }

  void generateURIAndConcatToFiles()
  {
    filePath = SCHEME + "://" + accessKey + ":" + secretKey + "@" + bucketName + "/" + directoryName + "/";
  }

  /**
   * Finalizing a file means that the same file will never be open again.
   *
   * @param fileName name of the file to finalize
   */
  @Override
  protected void finalizeFile(String fileName) throws IOException
  {
    String tmpFileName = getFileNameToTmpName().get(fileName);
    Path srcPath = new Path(filePath + Path.SEPARATOR + tmpFileName);
    Path destPath = new Path(filePath + Path.SEPARATOR + fileName);

    if (!fs.exists(destPath)) {
      LOG.debug("rename from tmp {} actual {} ", tmpFileName, fileName);
      fs.rename(srcPath, destPath);
    } else if (fs.exists(srcPath)) {
      //if the destination and src both exists that means there was a failure between file rename and clearing the endOffset so
      //we just delete the tmp file.
      LOG.debug("deleting tmp {}", tmpFileName);
      fs.delete(srcPath, true);
    }
    endOffsets.remove(fileName);
    getFileNameToTmpName().remove(fileName);

    //when writing to tmp files there can be vagrant tmp files which we have to clean
    FileStatus[] statuses = fs.listStatus(new Path(filePath));
    for (FileStatus status : statuses) {
      String statusName = status.getPath().getName();
      if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(fileName)) {
        LOG.debug("deleting vagrant file {}", statusName);
        fs.delete(status.getPath(), true);
      }
    }
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getDirectoryName() {
    return directoryName;
  }

  public void setDirectoryName(String directoryName) {
    this.directoryName = directoryName;
  }
}
