package com.datatorrent.apps.ingestion.lib;

import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
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
    // return new SecretKeySpec(key, algorithm);
    try {
      SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      KeySpec keySpec = new PBEKeySpec(CRYPTO_PASSWORD.toCharArray(),"passwordsalt".getBytes("UTF-8"),65536,128);
      Key genKey = factory.generateSecret(keySpec);
      Key secret = new SecretKeySpec(genKey.getEncoded(), "AES");
      return secret;
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
