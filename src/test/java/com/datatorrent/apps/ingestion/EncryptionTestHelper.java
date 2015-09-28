package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;

public class EncryptionTestHelper
{

  private static final String OUT_FILENAME = "dataFile.txt";
  public static final String FILE_DATA = "Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests. Test data for encryption tests.";

  public static void createFile(String dir)
  {
    try {
      File txtFile = new File(dir + File.separator + OUT_FILENAME);
      FileUtils.write(txtFile, FILE_DATA);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Key decryptSessionKey(byte[] encryptedKey, Key decrpytionKey) throws Exception
  {
    Cipher cipher = new CipherProvider(Application.RSA_TRANSFORMATION).getDecryptionCipher(decrpytionKey);
    byte[] keyBytes = cipher.doFinal(encryptedKey);
    return SymmetricKeyManager.getInstance().generateKey(keyBytes);
  }

  public static String decryptFileData(String transformation, Key secret, InputStream fileInputStream) throws Exception
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
