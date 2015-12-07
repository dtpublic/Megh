package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.LocalMode;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.lib.io.output.EncryptionMetaData;
import com.datatorrent.lib.io.output.SymmetricKeyManager;

public class EncryptionApplicationTest
{

  public static class TestMeta extends TestWatcher
  {
    String dataDirectory;
    String baseDirectory;
    String outputDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.outputDirectory = baseDirectory + "/output";
      this.dataDirectory = baseDirectory + "/data";
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSymmetricEncryption() throws Exception
  {
    Configuration conf = new Configuration(false);
    Map<String, String> configValues = new HashMap<String, String>();
    configValues.put("dt.application.Ingestion.encrypt.aes", "true");
    configValues.put("dt.application.Ingestion.encrypt.key", EncryptionTestHelper.SYMMETRIC_PASSKEY);
    LocalMode.Controller lc = createApplication(conf, configValues);

    long now = System.currentTimeMillis();

    Path outDir = new Path("file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    FileStatus[] statuses = fs.listStatus(new Path(outDir + File.separator + new File(testMeta.dataDirectory).getName()));
    Assert.assertTrue("file does not exist", statuses.length > 0 && fs.isFile(statuses[0].getPath()));
    Key secret = SymmetricKeyManager.getInstance().generateKey(EncryptionTestHelper.SYMMETRIC_PASSKEY.getBytes());
    FileInputStream fin = new FileInputStream(new File(statuses[0].getPath().toUri()));
    ObjectInputStream oin = new ObjectInputStream(fin);
    EncryptionMetaData metadata = (EncryptionMetaData) oin.readObject();
    String fileData = EncryptionTestHelper.decryptFileData((String) metadata.getMetadata().get(EncryptionMetaData.TRANSFORMATION), secret, fin);
    Assert.assertEquals("Data mismatch, error in encryption.", EncryptionTestHelper.FILE_DATA, fileData);
    fin.close();
    fs.close();
  }

  @Test
  public void testAsymmetricEncryption() throws Exception
  {
    Configuration conf = new Configuration(false);
    Map<String, String> configValues = new HashMap<String, String>();
    configValues.put("dt.application.Ingestion.encrypt.pki", "true");
    configValues.put("dt.application.Ingestion.encrypt.key", EncryptionTestHelper.PUBLIC_KEY);
    LocalMode.Controller lc = createApplication(conf, configValues);

    long now = System.currentTimeMillis();

    Path outDir = new Path("file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    FileStatus[] statuses = fs.listStatus(new Path(outDir + File.separator + new File(testMeta.dataDirectory).getName()));
    Assert.assertTrue("file does not exist", statuses.length > 0 && fs.isFile(statuses[0].getPath()));

    FileInputStream fin = new FileInputStream(new File(statuses[0].getPath().toUri()));
    ObjectInputStream oin = new ObjectInputStream(fin);
    EncryptionMetaData metaData = (EncryptionMetaData) oin.readObject();
    byte[] encryptedKey = (byte[]) metaData.getMetadata().get(EncryptionMetaData.KEY);

    Key privateKey = AsymmetricKeyManager.getInstance().generatePrivateKey(EncryptionTestHelper.PRIVATE_KEY.getBytes(), "RSA");
    String fileData = EncryptionTestHelper.decryptFileData(Application.AES_TRANSOFRMATION, EncryptionTestHelper.decryptSessionKey(encryptedKey, privateKey), fin);
    Assert.assertEquals("Data mismatch, error in encryption.", EncryptionTestHelper.FILE_DATA, fileData);
    oin.close();
    fs.close();
  }

  private LocalMode.Controller createApplication(Configuration conf, Map<String, String> configValues) throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAGContext.APPLICATION_PATH, testMeta.baseDirectory);

    conf.set("dt.operator.FileSplitter.prop.scanner.files", "file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegularExp", ".*?\\.txt");
    conf.set("dt.operator.FileMerger.prop.filePath", "file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.scanIntervalMillis", "10000");
    conf.set("dt.operator.BlockReader.prop.scheme", "file");
    conf.set("dt.output.protocol", "file");
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT", "10");
    conf.set("dt.application.Ingestion.attr.APPLICATION_PATH", testMeta.baseDirectory);
    conf.set("dt.application.Ingestion.attr.DEBUG", "false");

    Set<String> keyset = configValues.keySet();
    for (String key : keyset) {
      conf.set(key, configValues.get(key));
    }

    EncryptionTestHelper.createFile(testMeta.dataDirectory);
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    return lc;
  }

  private static final Logger LOG = LoggerFactory.getLogger(EncryptionApplicationTest.class);

}
