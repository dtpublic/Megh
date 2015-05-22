package com.datatorrent.apps.ingestion.lib;

import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

/**
 * Generates symmetric keys for crypto
 * 
 * @author Priyanka
 */
public class SymmetricKeyManager
{
  private static final String CRYPTO_PASSWORD = "dtsecretpassword";
  private static final String ALGORITHM = "AES";
  private static SymmetricKeyManager keyManager = new SymmetricKeyManager();

  private SymmetricKeyManager()
  {

  }

  public static SymmetricKeyManager getInstance()
  {
    return keyManager;
  }

  /**
   * Generates secrete key with product secret
   */
  public Key generateKey()
  {
    return generateKey(CRYPTO_PASSWORD.getBytes(), ALGORITHM);
  }

  /**
   * Generates secret from given key bytes
   *
   * @param key key bytes
   */
  public Key generateKey(byte[] key)
  {
    return generateKey(key, ALGORITHM);
  }

  /**
   * Generates secret from given key bytes
   *
   * @param key key bytes
   * @param algorithm the name of the secret-key algorithm to be associated with the given key material.
   */
  public Key generateKey(byte[] key, String algorithm)
  {
    return new SecretKeySpec(key, algorithm);
  }

}
