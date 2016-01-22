/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.output;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

/**
 * Generates symmetric keys for crypto
 *
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
   *          the name of the secret-key algorithm to be associated with the
   *          given key material.
   */
  public Key generateKey(byte[] key, String algorithm)
  {
    try {
      validateKey(key);
      return new SecretKeySpec(key, algorithm);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }

  }

  private void validateKey(byte[] key) throws InvalidKeyException
  {
    if (key.length != 16 && key.length != 24 && key.length != 32) {
      throw new InvalidKeyException("For AES encryption please provide key of size 128, 192 or 256 bits.");
    }

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
