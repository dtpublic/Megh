/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.security.Key;
import java.util.Queue;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.CryptoInformation;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;

/**
 * This operator merges the blocks into a file. The list of blocks is obtained from the FileMetadata. The implementation
 * extends AbstractReconciler, hence the file merging operation is carried out in a separate thread.
 * 
 */

public class FileMerger extends AbstractReconciler<FileMetadata, FileMetadata>
{
  protected transient FileSystem appFS, outputFS;

  @NotNull
  protected String filePath;
  protected String blocksDir;
  private transient String skippedListFile;

  private boolean deleteBlocks;
  private boolean overwriteOutputFile;
  private boolean encrypt;

  private CryptoInformation cryptoInformation;

  long skippedListFileLength;

  private Queue<Long> blocksMarkedForDeletion = Queues.newLinkedBlockingQueue();
  private Queue<Long> blocksSafeToDelete = Queues.newLinkedBlockingQueue();

  private static final String PART_FILE_EXTENTION = "._COPYING_";
  protected static final String STATS_DIR = "ingestionStats";
  protected static final String SKIPPED_FILE = "skippedFiles";
  protected static final String NEW_LINE_CHARACTER = "\n";

  private static final int BUFFER_SIZE = 64 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);
  protected final BasicCounters<MutableLong> mergerCounters;
  protected transient Context.OperatorContext context;

  public final transient DefaultOutputPort<FileMetadata> output = new DefaultOutputPort<FileMetadata>();

  public FileMerger()
  {
    deleteBlocks = true;
    mergerCounters = new BasicCounters<MutableLong>(MutableLong.class);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    mergerCounters.setCounter(Counters.TOTAL_DATA_INGESTED, new MutableLong());
    this.context = context;
    
    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
    skippedListFile = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + STATS_DIR + Path.SEPARATOR + SKIPPED_FILE;

    try {
      outputFS = getOutputFSInstance();
    } catch (IOException ex) {
      throw new RuntimeException("Exception in getting output file system.", ex);
    }
    try {
      appFS = getAppFSInstance();
    } catch (IOException ex) {
      try {
        outputFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception in closing output file system.", e);
      }
      throw new RuntimeException("Exception in getting application file system.", ex);
    }

    try {
      recoverSkippedListFile();
    } catch (IOException e) {
      throw new RuntimeException("Unable to recover skipped list file.", e);
    }
    super.setup(context); // Calling it at the end as the reconciler thread uses resources allocated above.
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    
    super.endWindow();
    context.setCounters(mergerCounters);
  }

  protected FileSystem getAppFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(blocksDir)).toUri(), new Configuration());
  }

  protected FileSystem getOutputFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
  }


  private void saveSkippedFiles(String fileName) throws IOException
  {
    FSDataOutputStream outStream = getStatsOutputStream();
    try {
      outStream.writeBytes(fileName + NEW_LINE_CHARACTER);
      skippedListFileLength = appFS.getFileStatus(new Path(skippedListFile)).getLen();
    } finally {
      outStream.close();
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    safelyDeleteBlocks();

    boolean gotException = false;
    try {
      if (appFS != null) {
        appFS.close();
        appFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }

    try {
      if (outputFS != null) {
        outputFS.close();
        outputFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }
    if (gotException) {
      throw new RuntimeException("Exception while closing file systems.");
    }
  }

  @VisibleForTesting
  protected void mergeFile(FileMetadata fmd) throws IOException
  {
    IngestionFileMetaData fileMetadata = null;
    if (fmd instanceof IngestionFileMetaData) {
      fileMetadata = (IngestionFileMetaData) fmd;
    }

    if (null == fileMetadata) {
      throw new RuntimeException("Input tuple is not an instance of IngestionFileMetaData.");
    }

    String absolutePath = filePath + Path.SEPARATOR + fileMetadata.getRelativePath();
    Path outputFilePath = new Path(absolutePath);
    LOG.debug("Processing file: {}", fileMetadata.getRelativePath());

    if (fileMetadata.isDirectory()) {
      createDir(outputFilePath);
      output.emit(fileMetadata);
      return;
    }

    if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
      LOG.debug("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
      saveSkippedFiles(absolutePath);
      output.emit(fileMetadata);
      markBlocksForDeletion(fileMetadata);
      return;
    }

    // All set to create file by merging blocks.
    mergeBlocks(fileMetadata);
    output.emit(fileMetadata);

    LOG.debug("Completed processing file: {} ", fileMetadata.getRelativePath());
    mergerCounters.getCounter(Counters.TOTAL_DATA_INGESTED).add(fileMetadata.getFileLength());
  }

  private void createDir(Path outputFilePath) throws IOException
  {
    if (!outputFS.exists(outputFilePath)) {
      outputFS.mkdirs(outputFilePath);
    }
  }

  protected void mergeBlocks(IngestionFileMetaData fileMetadata) throws IOException
  {
    String fileName = fileMetadata.getRelativePath();
    Path outputFilePath = new Path(filePath + Path.SEPARATOR + fileName);

    Path partFilePath = new Path(filePath, fileName + PART_FILE_EXTENTION);
    Path[] blockFiles = new Path[fileMetadata.getNumberOfBlocks()];
    int index = 0;
    for (long blockId : fileMetadata.getBlockIds()) {
      blockFiles[index] = new Path(blocksDir, Long.toString(blockId));
      index++;
    }
    OutputStream outputStream = getOutputStream(partFilePath);

    boolean writeException = false;
    try {
      writeBlocks(blockFiles, outputStream);
    } catch (IOException ex) {
      writeException = true;
    } finally {
      // TODO: Add to the list of failed files.
      outputStream.close();
      if (writeException && outputFS.exists(partFilePath)) {
        outputFS.delete(partFilePath, false);
      }
    }

    try {
      moveFile(partFilePath, outputFilePath);
    } finally {
      // Place holder to add this file to the list of failed files.
    }

    if (deleteBlocks) {
      markBlocksForDeletion(fileMetadata);
    }
  }

  protected FSDataOutputStream getOutputFSOutStream(Path partFilePath) throws IOException
  {
    return outputFS.create(partFilePath);
  }

  public void markBlocksForDeletion(IngestionFileMetaData fileMetadata)
  {
    for (long blockId : fileMetadata.getBlockIds()) {
      blocksMarkedForDeletion.add(blockId);
    }
  }

  private void safelyDeleteBlocks()
  {
    while (!blocksSafeToDelete.isEmpty()) {
      long blockId = blocksSafeToDelete.peek();
      Path blockPath = new Path(blocksDir, Long.toString(blockId));
      try {
        if (appFS.exists(blockPath)) { // takes care if blocks are deleted and then the operator is redeployed.
          appFS.delete(blockPath, false);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to delete block: " + blockId, e);
      }
      blocksSafeToDelete.remove();
    }
  }

  protected void writeBlocks(Path[] blockFiles, OutputStream outputStream) throws IOException
  {
    byte[] inputBytes = new byte[BUFFER_SIZE];
    int inputBytesRead;
    for (Path blockPath : blockFiles) {
      if (!appFS.exists(blockPath)) {
        throw new RuntimeException("Exception: Missing block " + blockPath);
      }
      InputStream inputStream = new DataInputStream(appFS.open(blockPath));
      LOG.debug("Writing block: {}", blockPath);
      try {
        while ((inputBytesRead = inputStream.read(inputBytes)) != -1) {
          outputStream.write(inputBytes, 0, inputBytesRead);
        }
      } finally {
        inputStream.close();
      }
    }
  }

  protected OutputStream getOutputStream(Path path) throws IOException
  {
    FSDataOutputStream outputStream = getOutputFSOutStream(path);
    if (isEncrypt()) {
      Cipher cipher;
      if (isPKI()) {
        cipher = getCipherForAsymmetricEncryption(outputStream);
      } else {
        cipher = getCipherForSymmetricEncryption(outputStream);
      }
      return new ObjectOutputStream(new CipherOutputStream(outputStream, cipher));
    }
    return outputStream;
  }

  private Cipher getCipherForSymmetricEncryption(FSDataOutputStream outputStream) throws IOException
  {
    EncryptionMetaData metaData = new EncryptionMetaData();
    metaData.setTransformation(cryptoInformation.getTransformation());
    writeMetadataToFile(outputStream, metaData);
    return new CipherProvider(cryptoInformation.getTransformation()).getEncryptionCipher(cryptoInformation.getSecretKey());
  }

  /*
   * generates symmetric session key and initializes cipher for symmetric encryption to encrypt file data. Given PKI
   * encryption key is used to encrypt session key and is stored in file as metadata.
   */
  private Cipher getCipherForAsymmetricEncryption(FSDataOutputStream outputStream) throws IOException
  {
    // create and encrypt session key
    Key sessionKey = SymmetricKeyManager.getInstance().generateRandomKey();
    byte[] encryptedSessionKey = encryptSessionkeyWithPKI(sessionKey);

    // write session key to file
    EncryptionMetaData metaData = new EncryptionMetaData();
    metaData.setTransformation(cryptoInformation.getTransformation());
    metaData.setKey(encryptedSessionKey);
    writeMetadataToFile(outputStream, metaData);
    return new CipherProvider(Application.AES_TRANSOFRMATION).getEncryptionCipher(sessionKey);
  }

  private byte[] encryptSessionkeyWithPKI(Key sessionKey)
  {
    try {
      Cipher rsaCipher = new CipherProvider(cryptoInformation.getTransformation()).getEncryptionCipher(cryptoInformation.getSecretKey());
      return rsaCipher.doFinal(sessionKey.getEncoded());
    } catch (BadPaddingException e) {
      throw new RuntimeException(e);
    } catch (IllegalBlockSizeException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeMetadataToFile(FSDataOutputStream outputStream, EncryptionMetaData metaData) throws IOException
  {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(metaData);
    oos.flush();
  }

  private boolean isPKI()
  {
    if (cryptoInformation.getTransformation().equals(Application.RSA_TRANSFORMATION)) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  protected void moveFile(Path source, Path destination) throws IOException
  {
    Path src = Path.getPathWithoutSchemeAndAuthority(source);
    Path dst = Path.getPathWithoutSchemeAndAuthority(destination);

    boolean moveSuccessful = false;
    if (!outputFS.exists(dst.getParent())) {
      outputFS.mkdirs(dst.getParent());
    }
    if (outputFS.exists(dst)) {
      outputFS.delete(dst, false);
    }
    moveSuccessful = outputFS.rename(src, dst);

    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      throw new RuntimeException("Unable to move file from " + src + " to " + dst);
    }
  }

  @Override
  public void committed(long l)
  {
    super.committed(l);
    safelyDeleteBlocks();
    movetoSafelyDelete();
  }

  private void movetoSafelyDelete()
  {
    while (!blocksMarkedForDeletion.isEmpty()) {
      blocksSafeToDelete.add(blocksMarkedForDeletion.remove());
    }
  }

  public boolean isDeleteSubFiles()
  {
    return deleteBlocks;
  }

  public void setDeleteSubFiles(boolean deleteBlocks)
  {
    this.deleteBlocks = deleteBlocks;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  protected void processTuple(FileMetadata fileMetadata)
  {
    enqueueForProcessing(fileMetadata);
  }

  @Override
  protected void processCommittedData(FileMetadata queueInput)
  {
    try {
      mergeFile(queueInput);
    } catch (IOException e) {
      throw new RuntimeException("Unable to merge file: " + queueInput.getFileName(), e);
    }
  }

  private void recoverSkippedListFile() throws IOException
  {
    FSDataOutputStream fsOutput = null;

    Path skippedListFilePath = new Path(skippedListFile);
    // Recovery is required only if the file length is more than what it was at checkpointing stage.
    if (appFS.exists(skippedListFilePath) && appFS.getFileStatus(skippedListFilePath).getLen() > skippedListFileLength) {
      Path partFilePath = new Path(skippedListFile + PART_FILE_EXTENTION);
      FSDataInputStream inputStream = appFS.open(skippedListFilePath);
      byte[] buffer = new byte[BUFFER_SIZE];
      try {
        fsOutput = appFS.create(partFilePath, true);
        while (inputStream.getPos() < skippedListFileLength) {
          long remainingBytes = skippedListFileLength - inputStream.getPos();
          int bytesToWrite = remainingBytes < BUFFER_SIZE ? (int) remainingBytes : BUFFER_SIZE;
          inputStream.read(buffer);
          fsOutput.write(buffer, 0, bytesToWrite);
        }
        FileContext fileContext = FileContext.getFileContext(appFS.getUri());
        LOG.debug("temp file path {}, skipped file path {}", partFilePath.toString(), skippedListFileLength);
        fileContext.rename(partFilePath, skippedListFilePath, Options.Rename.OVERWRITE);
      } finally {
        try {
          if (fsOutput != null) {
            fsOutput.close();
          }
        } finally {
          inputStream.close();
        }
      }
    }
  }

  private FSDataOutputStream getStatsOutputStream() throws IOException
  {
    Path skippedListFilePath = new Path(skippedListFile);
    if (appFS.exists(skippedListFilePath)) {
      return appFS.append(skippedListFilePath);
    }
    return appFS.create(skippedListFilePath);
  }

  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }
  
  
  public static enum Counters
  {
    TOTAL_DATA_INGESTED;
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
