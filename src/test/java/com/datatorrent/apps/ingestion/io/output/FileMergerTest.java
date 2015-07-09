/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io.output;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.common.BlockNotFoundException;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputFileBlockMetaData;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.CryptoInformation;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.google.common.collect.Lists;

public class FileMergerTest
{
  private static OperatorContext context;
  private static final long[] blockIds = new long[] { 1, 2, 3 };

  private static final String FILE_DATA = "0123456789";
  private static final String[] BLOCKS_DATA = {
    "0123",
    "4567",
    "89"
  };
  
  private static final String dummyDir = "dummpDir/anotherDummDir/";
  private static final String dummyFile = "dummy.txt";

  public static class TestFileMerger extends TestWatcher
  {
    public String recoveryDir = "";
    public String baseDir = "";
    public String blocksDir = "";
    public String outputDir = "";
    public String outputFileName = "";

    public File[] blockFiles = new File[blockIds.length];

    public IngestionFileMerger underTest;
    @Mock
    public IngestionFileSplitter.IngestionFileMetaData fileMetaDataMock;
    
    
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName() + Path.SEPARATOR;
      this.blocksDir = baseDir + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR;
      this.recoveryDir = baseDir + Path.SEPARATOR + "recovery";
      this.outputDir = baseDir + Path.SEPARATOR + "output" + Path.SEPARATOR;
      outputFileName = "output.txt";
      
      
      
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getMethodName());
      attributes.put(DAGContext.APPLICATION_PATH, baseDir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(baseDir).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.underTest = new IngestionFileMerger();
      this.underTest.setFilePath(outputDir);
      this.underTest.setup(context);

      MockitoAnnotations.initMocks(this);
      when(fileMetaDataMock.getFileName()).thenReturn(outputFileName);
      when(fileMetaDataMock.getRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getOutputRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(3);
      when(fileMetaDataMock.getBlockIds()).thenReturn(new long[] { 1, 2, 3 });
      when(fileMetaDataMock.isDirectory()).thenReturn(false);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(blockIds.length);
      
      List<OutputBlock> outputBlockMetaDataList = Lists.newArrayList();
      try{
        for(int i=0; i< blockIds.length; i++){
          blockFiles[i] = new File(blocksDir + blockIds[i]);
          FileUtils.write(blockFiles[i], BLOCKS_DATA[i]);
          FileBlockMetadata fmd = new FileBlockMetadata(blockFiles[i].getPath(), 
              blockIds[i], 0, BLOCKS_DATA[i].length(), (i==blockIds.length-1), -1);
          OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(fmd, outputFileName, (i==blockIds.length-1));
          outputBlockMetaDataList.add(outputFileBlockMetaData);
        }
      }
      catch(IOException e){
        throw new RuntimeException(e);
      }
      
      when(fileMetaDataMock.getOutputBlocksList()).thenReturn(outputBlockMetaDataList);

    }

    @Override
    protected void finished(Description description)
    {
      this.underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @AfterClass
  public static void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + FileMergerTest.class.getName()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Rule
  public TestFileMerger testFM = new TestFileMerger();

  @Test
  public void testMergeFile() throws IOException
  {
    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(testFM.outputDir, testFM.outputFileName)));
  }

  @Test
  public void testBlocksPath()
  {
    Assert.assertEquals("Blocks path not initialized in application context", context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR, testFM.blocksDir);
  }

  @Test
  public void testOverwriteFlag() throws IOException, InterruptedException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    long modTime = testFM.underTest.outputFS.getFileStatus(new Path(testFM.outputDir, testFM.outputFileName)).getModificationTime();
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});
    
    Thread.sleep(1000);
    testFM.underTest.setOverwriteOutputFile(true);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);
    FileStatus fileStatus = testFM.underTest.outputFS.getFileStatus(new Path(testFM.outputDir, testFM.outputFileName));
    Assert.assertTrue( fileStatus.getModificationTime() > modTime );
  }

  // Using a bit of reconciler during testing, so using committed call explicitly
  @Test
  public void testOverwriteFlagForDirectory() throws IOException, InterruptedException
  {
    FileUtils.forceMkdir(new File(testFM.outputDir + dummyDir));
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true);
    when(testFM.fileMetaDataMock.getOutputRelativePath()).thenReturn(dummyDir);
    testFM.underTest.setOverwriteOutputFile(true);

    testFM.underTest.beginWindow(1L);
    testFM.underTest.input.process(testFM.fileMetaDataMock);
    testFM.underTest.endWindow();
    testFM.underTest.checkpointed(1);
    testFM.underTest.committed(1);
    Thread.sleep(1000);
    
    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

@Test(expected = BlockNotFoundException.class)
  public void testMissingBlock() throws IOException, BlockNotFoundException
  {
    FileUtils.deleteQuietly(testFM.blockFiles[2]);
    testFM.underTest.writeTempOutputFile(testFM.fileMetaDataMock);
    fail("Failed when one block missing.");
  }

  @Test
  public void testDirectory() throws IOException
  {
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.getOutputRelativePath()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true); // is a directory
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @Test
  public void testFileWithRelativePath() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, dummyDir + dummyFile), FILE_DATA);
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir + dummyFile);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir + dummyFile);
    when(testFM.fileMetaDataMock.getOutputRelativePath()).thenReturn(dummyDir + dummyFile);

    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir + dummyFile);
    Assert.assertTrue(statsFile.exists() && !statsFile.isDirectory());
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(testFM.outputDir, dummyDir + dummyFile)));
  }
  
  private void writeBytesToBlockFiles() throws IOException{
    FileUtils.deleteQuietly(testFM.blockFiles[0]);
    FileUtils.deleteQuietly(testFM.blockFiles[1]);
    FileUtils.deleteQuietly(testFM.blockFiles[2]);
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[0]), BLOCKS_DATA[0].getBytes());
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[1]), BLOCKS_DATA[1].getBytes());
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[2]), BLOCKS_DATA[2].getBytes());
  }

  @Test
  public void testSymmetricEncryption() throws Exception
  {
    Key secret = SymmetricKeyManager.getInstance().generateRandomKey();
    testFM.underTest.setEncrypt(true);
    CryptoInformation cryptoInformation = new CryptoInformation(Application.AES_TRANSOFRMATION, secret);
    testFM.underTest.setCryptoInformation(cryptoInformation);
    writeBytesToBlockFiles();
    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);

    File encryptedFile = new File(testFM.outputDir, testFM.outputFileName);
    FileInputStream fin = new FileInputStream(encryptedFile);
    ObjectInputStream oin = new ObjectInputStream(fin);
    EncryptionMetaData metadata = (EncryptionMetaData) oin.readObject();
    try {
      String fileData = decryptFileData((String) metadata.getMetadata().get(EncryptionMetaData.TRANSFORMATION), secret, fin);
      Assert.assertEquals(FILE_DATA, fileData);
    } finally {
      oin.close();
    }
  }

  @Test
  public void testEncryptionWithUserKey() throws Exception
  {
    byte[] userKey = "passwordpassword".getBytes();
    Key secret = SymmetricKeyManager.getInstance().generateKey(userKey);
    testFM.underTest.setEncrypt(true);
    CryptoInformation cryptoInformation = new CryptoInformation(Application.AES_TRANSOFRMATION, secret);
    testFM.underTest.setCryptoInformation(cryptoInformation);
    writeBytesToBlockFiles();
    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);

    File encryptedFile = new File(testFM.outputDir, testFM.outputFileName);
    FileInputStream fin = new FileInputStream(encryptedFile);
    ObjectInputStream oin = new ObjectInputStream(fin);
    EncryptionMetaData metadata = (EncryptionMetaData) oin.readObject();
    try {
      String fileData = decryptFileData((String) metadata.getMetadata().get(EncryptionMetaData.TRANSFORMATION), secret, fin);
      Assert.assertEquals(FILE_DATA, fileData);
    } finally {
      oin.close();
    }
  }

  @Test
  public void testAssymetricEncryption() throws Exception
  {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(AsymmetricKeyManager.ALGORITHM);
    KeyPair pair = keyGen.generateKeyPair();
    Key privateKey = pair.getPrivate();
    Key publicKey = pair.getPublic();

    testFM.underTest.setEncrypt(true);
    CryptoInformation cryptoInformation = new CryptoInformation(Application.RSA_TRANSFORMATION, publicKey);
    testFM.underTest.setCryptoInformation(cryptoInformation);
    writeBytesToBlockFiles();
    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);

    File encryptedFile = new File(testFM.outputDir, testFM.outputFileName);
    FileInputStream fin = new FileInputStream(encryptedFile);
    ObjectInputStream oin = new ObjectInputStream(fin);
    try {
      EncryptionMetaData metaData = (EncryptionMetaData) oin.readObject();
      byte[] encryptedKey = (byte[]) metaData.getMetadata().get(EncryptionMetaData.KEY);

      String fileData = decryptFileData(Application.AES_TRANSOFRMATION, getSessionKey(encryptedKey, privateKey), fin);
      Assert.assertEquals(FILE_DATA, fileData);
    } finally {
      oin.close();
    }
  }

  private Key getSessionKey(byte[] encryptedKey, Key decrpytionKey) throws Exception
  {
    Cipher cipher = new CipherProvider(Application.RSA_TRANSFORMATION).getDecryptionCipher(decrpytionKey);
    byte[] keyBytes = cipher.doFinal(encryptedKey);
    return SymmetricKeyManager.getInstance().generateKey(keyBytes);
  }

  private String decryptFileData(String transformation, Key secret, InputStream fileInputStream) throws Exception
  {
    Cipher cipher = new CipherProvider(transformation).getDecryptionCipher(secret);
    CipherInputStream cin = new CipherInputStream(fileInputStream, cipher);
    ObjectInputStream oin = new ObjectInputStream(cin);
    String fileData;
    try {
      fileData = IOUtils.toString(oin);
    } finally {
      oin.close();
    }
    return fileData;
  }

}
