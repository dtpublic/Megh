package com.datatorrent.apps.ingestion.io.output;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
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
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.lib.AESCryptoProvider;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.FileSplitter;

public class FileMergerTest
{
  private static OperatorContext context;
  private static long[] blockIds = new long[] { 1, 2, 3 };

  private static final String FILE_DATA = "0123456789";
  private static final String BLOCK1_DATA = "0123";
  private static final String BLOCK2_DATA = "4567";
  private static final String BLOCK3_DATA = "89";
  private static final String dummyDir = "dummpyDir/anotherDummyDir/";
  private static final String dummyFile = "dummy.txt";

  public static class TestFileMerger extends TestWatcher
  {
    public String recoveryDir = "";
    public String baseDir = "";
    public String blocksDir = "";
    public String outputDir = "";
    public String statsDir = "";
    public String outputFileName = "";

    public FileMerger underTest;
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
      this.statsDir = baseDir + Path.SEPARATOR + FileMerger.STATS_DIR + Path.SEPARATOR;
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

      this.underTest = new FileMerger();
      this.underTest.setFilePath(outputDir);
      this.underTest.setup(context);

      MockitoAnnotations.initMocks(this);
      when(fileMetaDataMock.getFileName()).thenReturn(outputFileName);
      when(fileMetaDataMock.getRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(3);
      when(fileMetaDataMock.getBlockIds()).thenReturn(new long[] { 1, 2, 3 });
      when(fileMetaDataMock.isDirectory()).thenReturn(false);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(blockIds.length);
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
    FileUtils.write(new File(testFM.blocksDir + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(testFM.blocksDir + blockIds[1]), BLOCK2_DATA);
    FileUtils.write(new File(testFM.blocksDir + blockIds[2]), BLOCK3_DATA);
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(testFM.outputDir, testFM.outputFileName)));
  }

  @Test
  public void testBlocksPath()
  {
    Assert.assertEquals("Blocks path not initialized in application context", context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR, testFM.blocksDir);
  }

  @Test
  public void testOverwriteFlag() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    testFM.underTest.setOverwriteOutputFile(true);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + FileMerger.SKIPPED_FILE);
    Assert.assertFalse(statsFile.exists());

  }

  @Test
  public void testSkippedFilePersistance() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);

    testFM.underTest.setOverwriteOutputFile(false);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);

    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + FileMerger.SKIPPED_FILE);
    Assert.assertTrue(statsFile.exists());
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(testFM.outputFileName));
  }

  @Test
  public void testSkippedFileRecovery() throws IOException
  {
    testFM.underTest.skippedListFileLength = 12;
    String skippedFileNames = "skippedFile1\nskippedFile2";
    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + FileMerger.SKIPPED_FILE);
    FileUtils.write(statsFile, skippedFileNames);
    testFM.underTest.setup(context);
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(skippedFileNames.substring(0, (int) testFM.underTest.skippedListFileLength)));
  }

  // Using a bit of reconciler during testing, so using committed call explicitly
  @Test
  public void testOverwriteFlagForDirectory() throws IOException, InterruptedException
  {
    FileUtils.forceMkdir(new File(testFM.outputDir + dummyDir));
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir);
    testFM.underTest.setOverwriteOutputFile(true);
    testFM.underTest.beginWindow(1L);
    testFM.underTest.input.process(testFM.fileMetaDataMock);
    testFM.underTest.endWindow();
    testFM.underTest.checkpointed(1);
    testFM.underTest.committed(1);
    Thread.sleep(1000L);

    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @Test(expected = RuntimeException.class)
  public void testMissingBlock() throws IOException
  {
    FileUtils.write(new File(testFM.blocksDir + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(testFM.blocksDir + blockIds[1]), BLOCK2_DATA);
    // FileUtils.write(new File(testFM.blocksDir + blockIds[2]), BLOCK3_DATA); //Missing block, should throw exception
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
    fail("Failed when one block missing.");
  }

  @Test(expected = RuntimeException.class)
  public void testNullMetaData() throws IOException
  {
    testFM.underTest.mergeFile(null);
    fail("Failed when FileMetadata is null.");
  }

  @Test(expected = RuntimeException.class)
  public void testFileMetaDataInstance() throws IOException
  {
    testFM.underTest.mergeFile(new FileSplitter.FileMetadata("tmp"));
    fail("Failed when FileMetadata is not instance of IngestionFileMetaData.");
  }

  @Test
  public void testDirectory() throws IOException
  {
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true); // is a directory
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @Test
  public void testFileWithRelativePath() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, dummyDir + dummyFile), FILE_DATA);
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir + dummyFile);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir + dummyFile);

    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir + dummyFile);
    Assert.assertTrue(statsFile.exists() && !statsFile.isDirectory());
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(testFM.outputDir, dummyDir + dummyFile)));
  }

  @Test
  public void testEncryption() throws Exception
  {
    SecretKey secret = SymmetricKeyManager.getInstance().generateSymmetricKeyForAES();
    testFM.underTest.setEncrypt(true);
    testFM.underTest.setSecret(secret);

    AESCryptoProvider cryptoProvider = new AESCryptoProvider();
    Cipher cipher = cryptoProvider.getEncryptionCipher(secret);

    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[0]), cipher.doFinal(BLOCK1_DATA.getBytes()));
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[1]), cipher.doFinal(BLOCK2_DATA.getBytes()));
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[2]), cipher.doFinal(BLOCK3_DATA.getBytes()));
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);

    String fileData = decryptFileData(secret);

    Assert.assertEquals(FILE_DATA, fileData);
  }

  @Test
  public void testEncryptionWithUserKey() throws Exception
  {
    byte[] userKey = "passwordpassword".getBytes();
    SecretKey secret = SymmetricKeyManager.getInstance().generateSymmetricKeyForAES(userKey);
    testFM.underTest.setEncrypt(true);
    testFM.underTest.setSecret(secret);

    Cipher cipher = new AESCryptoProvider().getEncryptionCipher(secret);

    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[0]), cipher.doFinal(BLOCK1_DATA.getBytes()));
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[1]), cipher.doFinal(BLOCK2_DATA.getBytes()));
    FileUtils.writeByteArrayToFile(new File(testFM.blocksDir + blockIds[2]), cipher.doFinal(BLOCK3_DATA.getBytes()));
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);

    String fileData = decryptFileData(secret);

    Assert.assertEquals(FILE_DATA, fileData);
  }

  private String decryptFileData(SecretKey secret) throws Exception
  {
    Cipher cipher = new AESCryptoProvider().getDecryptionCipher(secret);
    File encryptedFile = new File(testFM.outputDir, testFM.outputFileName);
    CipherInputStream cin = new CipherInputStream(new FileInputStream(encryptedFile), cipher);
    StringBuilder readData = new StringBuilder();
    try {
      byte[] data = new byte[4];
      int readBytes;
      while ((readBytes = cin.read(data)) != -1) {
        readData.append(new String(Arrays.copyOf(data, readBytes), "UTF-8"));
      }
    } finally {
      cin.close();
    }
    return readData.toString();
  }
}
