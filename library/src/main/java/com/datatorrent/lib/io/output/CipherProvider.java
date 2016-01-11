package com.datatorrent.lib.io.output;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

/**
 * Provides encrypt decrypt ciphers for selected algorithm
 *
 * @author Priyanka
 * @since 1.0.0
 */
public class CipherProvider
{
  private Cipher cipher;

  public CipherProvider(String transformation)
  {
    try {
      cipher = Cipher.getInstance(transformation);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (NoSuchPaddingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initializes cipher with given key in encryption mode
   *
   * @param secret secret key
   */
  public Cipher getEncryptionCipher(Key secret)
  {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, secret);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return cipher;
  }

  /**
   * Initializes cipher with given key in decrypt mode
   *
   * @param secret secret key
   */
  public Cipher getDecryptionCipher(Key secret)
  {
    try {
      cipher.init(Cipher.DECRYPT_MODE, secret);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return cipher;
  }

}
