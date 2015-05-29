package com.datatorrent.apps.ingestion.lib;

import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

/**
 * Generates symmetric keys for crypto
 * 
 * @author Priyanka
 */
public class SymmetricKeyManager
{
  private static final String ALGORITHM = "AES";
  private static final int DEFAULT_KEY_SIZE = 128;
  private int keySize = DEFAULT_KEY_SIZE;

  private static SymmetricKeyManager keyManager = new SymmetricKeyManager();

  private SymmetricKeyManager()
  {

  }

  public static SymmetricKeyManager getInstance()
  {
    return keyManager;
  }

  /**
   * Generates random secrete key
   */
  public Key generateRandomKey()
  {
    KeyGenerator keyGen;
    try {
      keyGen = KeyGenerator.getInstance(ALGORITHM);
      keyGen.init(keySize);
      return keyGen.generateKey();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates secret from given key bytes
   *
   * @param key
   *          key bytes
   */
  public Key generateKey(byte[] key)
  {
    return generateKey(key, ALGORITHM);
  }

  /**
   * Generates secret from given key bytes
   *
   * @param key
   *          key bytes
   * @param algorithm
   *          the name of the secret-key algorithm to be associated with the given key material.
   */
  public Key generateKey(byte[] key, String algorithm)
  {
    return new SecretKeySpec(key, algorithm);
  }

  public int getKeySize()
  {
    return keySize;
  }

  public void setKeySize(int keySize)
  {
    this.keySize = keySize;
  }

}
