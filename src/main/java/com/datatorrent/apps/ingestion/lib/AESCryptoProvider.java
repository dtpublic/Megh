package com.datatorrent.apps.ingestion.lib;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

/**
 * Provides encrypt decrypt functions for AES algorithm of different key sizes.
 * 
 * @author Priyanka
 * 
 */
public class AESCryptoProvider
{
  private static final String TRANSFORMATION = "AES/ECB/PKCS5Padding";
  private Cipher cipher;

  public AESCryptoProvider()
  {
    this(TRANSFORMATION);
  }

  public AESCryptoProvider(String transformation)
  {
    try {
      cipher = Cipher.getInstance(TRANSFORMATION);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (NoSuchPaddingException e) {
      throw new RuntimeException(e);
    }
  }

  public Cipher getEncryptionCipher(SecretKey secret)
  {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, secret);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return cipher;
  }

  public Cipher getDecryptionCipher(SecretKey secret)
  {
    try {
      cipher.init(Cipher.DECRYPT_MODE, secret);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
    return cipher;
  }

}
