package com.datatorrent.apps.ingestion.lib;

import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

public class AsymmetricKeyManager
{
  public static final String ALGORITHM = "RSA";
  private static AsymmetricKeyManager keyManager = new AsymmetricKeyManager();

  private AsymmetricKeyManager()
  {

  }

  public static AsymmetricKeyManager getInstance()
  {
    return keyManager;
  }

  /**
   * Generates public key from give key bytes
   * 
   * @param key The key, which is assumed to be encoded according to the X.509 standard
   */
  public Key generatePublicKey(byte[] key)
  {
    return generatePublicKey(key, ALGORITHM);
  }

  /**
   * Generates public key from given key bytes
   * 
   * @param key The key, which is assumed to be encoded according to the X.509 standard
   * @param algorithm he name of the requested key algorithm
   */
  public Key generatePublicKey(byte[] key, String algorithm)
  {
    X509EncodedKeySpec ks = new X509EncodedKeySpec(key);
    try {
      return KeyFactory.getInstance(algorithm).generatePublic(ks);
    } catch (InvalidKeySpecException e) {
      e.printStackTrace();
      throw new RuntimeException();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException();
    }
  }
}
