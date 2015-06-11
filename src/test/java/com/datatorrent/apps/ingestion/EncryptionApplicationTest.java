package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import com.datatorrent.apps.ingestion.io.output.EncryptionMetaData;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;

public class EncryptionApplicationTest
{

  private static final String OUT_FILENAME = "dataFile.txt";
  private static final String FILE_DATA = "Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests.";

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
    String SYMMETRIC_PASSKEY = "Dtsecretpassword";

    Configuration conf = new Configuration(false);
    Map<String, String> configValues = new HashMap<String, String>();
    configValues.put("dt.application.Ingestion.encrypt.aes", "true");
    configValues.put("dt.application.Ingestion.encrypt.key", SYMMETRIC_PASSKEY);
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
    Key secret = SymmetricKeyManager.getInstance().generateKey(SYMMETRIC_PASSKEY.getBytes());
    FileInputStream fin = new FileInputStream(new File(statuses[0].getPath().toUri()));
    ObjectInputStream oin = new ObjectInputStream(fin);
    EncryptionMetaData metadata = (EncryptionMetaData) oin.readObject();
    String fileData = decryptFileData((String) metadata.getMetadata().get(EncryptionMetaData.TRANSFORMATION), secret, fin);
    Assert.assertEquals("Data mismatch, error in encryption.", FILE_DATA, fileData);
    fin.close();
    fs.close();
  }

  @Test
  public void testAsymmetricEncryption() throws Exception
  {
    String PUBLIC_KEY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs6IVc+ahnabiIAD+gQvqHikkkcb1uUglN52M8691e+jHPtOhVWUVJdMq/NcduZJifQo27shz9VC6/DS88rS9dIsYkZJkpiHYcxc0oTOESu3e9EJNOLomNCW1xPdq0b8ACtslbJT5SeQantuJTdZxwuPv5qLb9ar9wDkPaQg9//PgKVJUcAfsEle1B7Ig/u5Q0GZiXk/DPo3m/EsQhTvyp7XVXtcxd7sdxbtXjR/sWU/lM31sjO2Lt/a66ZejDWxEuDK7QDnwEKqASyS8SzZ738eFWeuwAzl8bWTLDlAxAAQlf/MC9SWfej+s/jEvnsaTxm/chu8xX5KXyzYn2tNmoQIDAQAB";
    String PRIVATE_KEY = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCzohVz5qGdpuIgAP6BC+oeKSSRxvW5SCU3nYzzr3V76Mc+06FVZRUl0yr81x25kmJ9CjbuyHP1ULr8NLzytL10ixiRkmSmIdhzFzShM4RK7d70Qk04uiY0JbXE92rRvwAK2yVslPlJ5Bqe24lN1nHC4+/motv1qv3AOQ9pCD3/8+ApUlRwB+wSV7UHsiD+7lDQZmJeT8M+jeb8SxCFO/KntdVe1zF3ux3Fu1eNH+xZT+UzfWyM7Yu39rrpl6MNbES4MrtAOfAQqoBLJLxLNnvfx4VZ67ADOXxtZMsOUDEABCV/8wL1JZ96P6z+MS+expPGb9yG7zFfkpfLNifa02ahAgMBAAECggEAQiLd5SM3w7SKsp/LFDYPx3T8atOm6sWeNkDCgYHLLojAuufuEWO7CocZ36dP4V/89O6K1RVmZB6KCYtdObrDuiHwNMCCYAw8Bfu4O9Wc0n2LxcCXb9hRcoejydYSNREucdDHkZezxLm/91b60XavwcJsNC0n8OY4sMoRW2lWcmJ4gBM4n8QzG1dsJS+QZCipva7ptcxU9JdBXqWKYeVKeA9Ze8qMsbgkLPkZusjPJVXOPfEya7c22nV+ozglM8PpLub43XF0N8BYt1ks8d0osUmRcNN3EE1uumu3Tq6dAppwbeApheX1bIRIGX/XnbUmMSKREYeZ9Cns8JwJpybZFQKBgQDnB7XSxTb1t7h6A7kkyG5rwsyEqXGtmWeyCATAKZiu4rW0dh0dRZGQ2WMHSnsWGhqZzlYwJZ99J3138OYzegdbQnOOzsYJHVs6nTEw+8hCPn9PsOx1fFjRG3fZGzKSOpXUzW6fgpsNjDzFrgtHtHRwyL07SbmwQj+H8Gpfa7QzYwKBgQDHDEn3ehEcC1dVnjf38K1OGHKgnoil0gErRMVdMOUdaXZjcM5GRUWio9M9be+lbjAsrtJrK/gUuK42mojse+c7D4gnjDeswR69HY4nxSzdJBtVboyque0KK/hdLVTmUgD+PxlliXFAhFTztRwqQGY7fwreOOetcbGPlsyQtdG3KwKBgHPj9z5qdX5fEagLNBWSgWmHBybJBOBLYqv4v8FRXGjmCrYixcoIOtQJaFag8wuMPqnGyo9OYCnc5GCFNETAQu5xcBxD9y1dT4Ugkyt6MeOhDCYCnyr0HG2QtNbwgLa/sqdUAdj8ICF0pouXGct3Zy2oVNxnyED1in77h7CkC3n3AoGAG1jI4MNYjm3Qdebi8aGTbeNV/FNLmtybZIJySzdogv32Ufsxm93wj0PKxenQvv3AiKMMLcVAtDgbV00r+rGbNzYPEr/k9ksiGgFxgm1ImKlZSAeENACPXJJl8QdFXs9ta4Dn0Fdtw9tqgIEleXiXkx0FNTrEOcQhDQU+3bLdOTkCgYAQ6myMOgfpSkhmnB4RHTRMfkRCMoeLYzerlwlZ/FaBb46MjvhygJ26EEZhaLOclDuQWKfaDK0p6r06aYSec8zCPGpq7ORqfr5hLPNjWsqJwmaZDIf6Y0Ln4JHsZq8YRt/IHUYyts/+jaHZi4IM5JWmd0ZMLsrpK3eJ46y3ABJi3g==";

    Configuration conf = new Configuration(false);
    Map<String, String> configValues = new HashMap<String, String>();
    configValues.put("dt.application.Ingestion.encrypt.pki", "true");
    configValues.put("dt.application.Ingestion.encrypt.key", PUBLIC_KEY);
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

    Key privateKey = AsymmetricKeyManager.getInstance().generatePrivateKey(PRIVATE_KEY.getBytes(), "RSA");
    String fileData = decryptFileData(Application.AES_TRANSOFRMATION, decryptSessionKey(encryptedKey, privateKey), fin);
    Assert.assertEquals("Data mismatch, error in encryption.", FILE_DATA, fileData);
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

    createFile();
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();
    return lc;
  }

  private void createFile()
  {
    try {
      File txtFile = new File(testMeta.dataDirectory + File.separator + OUT_FILENAME);
      FileUtils.write(txtFile, FILE_DATA);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Key decryptSessionKey(byte[] encryptedKey, Key decrpytionKey) throws Exception
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

  private static final Logger LOG = LoggerFactory.getLogger(EncryptionApplicationTest.class);

}
