/*
 * Copyright (c) 2016 DataTorrent, Inc. 
 * ALL Rights Reserved.
 *
 */
package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.output.FilterStreamProviders.TimedCipherOutputStream;
import com.datatorrent.lib.io.output.TrackerEvent.TrackerEventType;

/**
 * This operator merges the blocks into a file. The list of blocks is obtained
 * from the IngestionFileMetaData. The implementation extends OutputFileMerger
 * (which uses reconsiler), hence the file merging operation is carried out in a
 * separate thread.
 *
 */
public class FileMerger extends FileStitcher<ExtendedModuleFileMetaData>
{
  private boolean overwriteOutputFile;
  private boolean encrypt;

  private CryptoInformation cryptoInformation;

  private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);

  public final transient DefaultOutputPort<TrackerEvent> trackerOutPort = new DefaultOutputPort<TrackerEvent>();

  @AutoMetric
  private long bytesWrittenPerSec;

  private long bytesWritten;
  private double windowTimeSec;

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
    bytesWrittenPerSec = 0;
    bytesWritten = 0;
  }

  /* 
   * Calls super.endWindow() and sets counters 
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    ExtendedModuleFileMetaData tuple;
    int size = doneTuples.size();
    for (int i = 0; i < size; i++) {
      tuple = doneTuples.peek();
      // If a tuple is present in doneTuples, it has to be also present in successful/failed/skipped
      // as processCommittedData adds tuple in successful/failed/skipped
      // and then reconciler thread add that in doneTuples 
      if (successfulFiles.contains(tuple)) {
        successfulFiles.remove(tuple);
        trackerOutPort.emit(new TrackerEvent(TrackerEventType.SUCCESSFUL_FILE, tuple.getFilePath()));
        tuple.setCompletionStatus(TrackerEventType.SUCCESSFUL_FILE);
        LOG.debug("File copy successful: {}", tuple.getOutputRelativePath());
      } else if (skippedFiles.contains(tuple)) {
        skippedFiles.remove(tuple);
        trackerOutPort.emit(new TrackerEvent(TrackerEventType.SKIPPED_FILE, tuple.getFilePath()));
        tuple.setCompletionStatus(TrackerEventType.SKIPPED_FILE);
        LOG.debug("File copy skipped: {}", tuple.getOutputRelativePath());
      } else if (failedFiles.contains(tuple)) {
        failedFiles.remove(tuple);
        trackerOutPort.emit(new TrackerEvent(TrackerEventType.FAILED_FILE, tuple.getFilePath()));
        tuple.setCompletionStatus(TrackerEventType.FAILED_FILE);
        LOG.debug("File copy failed: {}", tuple.getOutputRelativePath());
      } else {
        throw new RuntimeException(
            "Tuple present in doneTuples but not in successfulFiles: " + tuple.getOutputRelativePath());
      }
      completedFilesMetaOutput.emit(tuple);
      committedTuples.remove(tuple);
      doneTuples.poll();
    }

    bytesWrittenPerSec = (long)(bytesWritten / windowTimeSec);
  }

  @Override
  protected void mergeOutputFile(ExtendedModuleFileMetaData moduleFileMetaData) throws IOException
  {
    LOG.debug("Processing file: {}", moduleFileMetaData.getOutputRelativePath());

    Path outputFilePath = new Path(filePath, moduleFileMetaData.getOutputRelativePath());
    if (moduleFileMetaData.isDirectory()) {
      createDir(outputFilePath);
      successfulFiles.add(moduleFileMetaData);
      return;
    }

    if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
      LOG.debug("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
      skippedFiles.add(moduleFileMetaData);
      return;
    }
    //Call super method for serial merge of blocks
    super.mergeOutputFile(moduleFileMetaData);
    moduleFileMetaData.setCompletionTime(System.currentTimeMillis());

    Path destination = new Path(filePath, moduleFileMetaData.getOutputRelativePath());
    Path path = Path.getPathWithoutSchemeAndAuthority(destination);
    long len = outputFS.getFileStatus(path).getLen();
    moduleFileMetaData.setOutputFileSize(len);
  }

  /* (non-Javadoc)
   * @see com.datatorrent.apps.ingestion.io.output.OutputFileMerger#writeTempOutputFile(com.datatorrent.apps.ingestion.io.output.OutputFileMetaData)
   */
  @Override
  protected OutputStream writeTempOutputFile(ExtendedModuleFileMetaData moduleFileMetadata)
      throws IOException, BlockNotFoundException
  {
    OutputStream outputStream = super.writeTempOutputFile(moduleFileMetadata);
    if (isEncrypt() && outputStream instanceof TimedCipherOutputStream) {
      TimedCipherOutputStream timedCipherOutputStream = (TimedCipherOutputStream)outputStream;
      moduleFileMetadata.setEncryptionTime(timedCipherOutputStream.getTimeTaken());
      LOG.debug("Adding to counter TIME_TAKEN_FOR_ENCRYPTION : {}", timedCipherOutputStream.getTimeTaken());
    }
    bytesWritten += moduleFileMetadata.getFileLength();
    return outputStream;
  }

  private void createDir(Path outputFilePath) throws IOException
  {
    if (!outputFS.exists(outputFilePath)) {
      outputFS.mkdirs(outputFilePath);
    }
  }

  @Override
  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    OutputStream outputStream = outputFS.create(partFilePath);
    if (isEncrypt()) {
      return getCipherOutputStream(outputStream);
    }
    return outputStream;
  }

  @SuppressWarnings("resource")
  protected TimedCipherOutputStream getCipherOutputStream(OutputStream outputStream) throws IOException
  {
    EncryptionMetaData metaData = new EncryptionMetaData();
    metaData.setTransformation(cryptoInformation.getTransformation());
    Cipher cipher;
    if (isPKI()) {
      Key sessionKey = SymmetricKeyManager.getInstance().generateRandomKey();
      byte[] encryptedSessionKey = encryptSessionkeyWithPKI(sessionKey);
      metaData.setKey(encryptedSessionKey);
      cipher = new CipherProvider(FilterStreamProviders.AES_TRANSOFRMATION).getEncryptionCipher(sessionKey);
    } else {
      cipher = new CipherProvider(cryptoInformation.getTransformation())
          .getEncryptionCipher(cryptoInformation.getSecretKey());
    }
    return new FilterStreamProviders.TimedCipherOutputStream(outputStream, cipher, metaData);
  }

  private byte[] encryptSessionkeyWithPKI(Key sessionKey)
  {
    try {
      Cipher rsaCipher = new CipherProvider(cryptoInformation.getTransformation())
          .getEncryptionCipher(cryptoInformation.getSecretKey());
      return rsaCipher.doFinal(sessionKey.getEncoded());
    } catch (BadPaddingException e) {
      throw new RuntimeException(e);
    } catch (IllegalBlockSizeException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isPKI()
  {
    if (cryptoInformation.getTransformation().equals(FilterStreamProviders.RSA_TRANSFORMATION)) {
      return true;
    }
    return false;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }

  public boolean isEncrypt()
  {
    return encrypt;
  }

  public void setEncrypt(boolean encrypt)
  {
    this.encrypt = encrypt;
  }

  public void setCryptoInformation(CryptoInformation cipherProvider)
  {
    this.cryptoInformation = cipherProvider;
  }

}
