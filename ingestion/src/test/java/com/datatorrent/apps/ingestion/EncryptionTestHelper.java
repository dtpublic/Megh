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
  
  public static final String SYMMETRIC_PASSKEY = "Dtsecretpassword";
  public static final String PUBLIC_KEY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs6IVc+ahnabiIAD+gQvqHikkkcb1uUglN52M8691e+jHPtOhVWUVJdMq/NcduZJifQo27shz9VC6/DS88rS9dIsYkZJkpiHYcxc0oTOESu3e9EJNOLomNCW1xPdq0b8ACtslbJT5SeQantuJTdZxwuPv5qLb9ar9wDkPaQg9//PgKVJUcAfsEle1B7Ig/u5Q0GZiXk/DPo3m/EsQhTvyp7XVXtcxd7sdxbtXjR/sWU/lM31sjO2Lt/a66ZejDWxEuDK7QDnwEKqASyS8SzZ738eFWeuwAzl8bWTLDlAxAAQlf/MC9SWfej+s/jEvnsaTxm/chu8xX5KXyzYn2tNmoQIDAQAB";
  public static final String PRIVATE_KEY = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCzohVz5qGdpuIgAP6BC+oeKSSRxvW5SCU3nYzzr3V76Mc+06FVZRUl0yr81x25kmJ9CjbuyHP1ULr8NLzytL10ixiRkmSmIdhzFzShM4RK7d70Qk04uiY0JbXE92rRvwAK2yVslPlJ5Bqe24lN1nHC4+/motv1qv3AOQ9pCD3/8+ApUlRwB+wSV7UHsiD+7lDQZmJeT8M+jeb8SxCFO/KntdVe1zF3ux3Fu1eNH+xZT+UzfWyM7Yu39rrpl6MNbES4MrtAOfAQqoBLJLxLNnvfx4VZ67ADOXxtZMsOUDEABCV/8wL1JZ96P6z+MS+expPGb9yG7zFfkpfLNifa02ahAgMBAAECggEAQiLd5SM3w7SKsp/LFDYPx3T8atOm6sWeNkDCgYHLLojAuufuEWO7CocZ36dP4V/89O6K1RVmZB6KCYtdObrDuiHwNMCCYAw8Bfu4O9Wc0n2LxcCXb9hRcoejydYSNREucdDHkZezxLm/91b60XavwcJsNC0n8OY4sMoRW2lWcmJ4gBM4n8QzG1dsJS+QZCipva7ptcxU9JdBXqWKYeVKeA9Ze8qMsbgkLPkZusjPJVXOPfEya7c22nV+ozglM8PpLub43XF0N8BYt1ks8d0osUmRcNN3EE1uumu3Tq6dAppwbeApheX1bIRIGX/XnbUmMSKREYeZ9Cns8JwJpybZFQKBgQDnB7XSxTb1t7h6A7kkyG5rwsyEqXGtmWeyCATAKZiu4rW0dh0dRZGQ2WMHSnsWGhqZzlYwJZ99J3138OYzegdbQnOOzsYJHVs6nTEw+8hCPn9PsOx1fFjRG3fZGzKSOpXUzW6fgpsNjDzFrgtHtHRwyL07SbmwQj+H8Gpfa7QzYwKBgQDHDEn3ehEcC1dVnjf38K1OGHKgnoil0gErRMVdMOUdaXZjcM5GRUWio9M9be+lbjAsrtJrK/gUuK42mojse+c7D4gnjDeswR69HY4nxSzdJBtVboyque0KK/hdLVTmUgD+PxlliXFAhFTztRwqQGY7fwreOOetcbGPlsyQtdG3KwKBgHPj9z5qdX5fEagLNBWSgWmHBybJBOBLYqv4v8FRXGjmCrYixcoIOtQJaFag8wuMPqnGyo9OYCnc5GCFNETAQu5xcBxD9y1dT4Ugkyt6MeOhDCYCnyr0HG2QtNbwgLa/sqdUAdj8ICF0pouXGct3Zy2oVNxnyED1in77h7CkC3n3AoGAG1jI4MNYjm3Qdebi8aGTbeNV/FNLmtybZIJySzdogv32Ufsxm93wj0PKxenQvv3AiKMMLcVAtDgbV00r+rGbNzYPEr/k9ksiGgFxgm1ImKlZSAeENACPXJJl8QdFXs9ta4Dn0Fdtw9tqgIEleXiXkx0FNTrEOcQhDQU+3bLdOTkCgYAQ6myMOgfpSkhmnB4RHTRMfkRCMoeLYzerlwlZ/FaBb46MjvhygJ26EEZhaLOclDuQWKfaDK0p6r06aYSec8zCPGpq7ORqfr5hLPNjWsqJwmaZDIf6Y0Ln4JHsZq8YRt/IHUYyts/+jaHZi4IM5JWmd0ZMLsrpK3eJ46y3ABJi3g==";

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
