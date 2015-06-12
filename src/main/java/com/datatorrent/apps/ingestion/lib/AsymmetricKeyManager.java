package com.datatorrent.apps.ingestion.lib;

import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.xml.bind.DatatypeConverter;

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
   * @param key Public key which is according to the X.509 standard
   * @param algorithm the name of the requested key algorithm
   */
  public Key generatePublicKey(byte[] key, String algorithm)
  {
    byte[] decodedKey = DatatypeConverter.parseBase64Binary(new String(key));
    X509EncodedKeySpec ks = new X509EncodedKeySpec(decodedKey);
    try {
      return KeyFactory.getInstance(algorithm).generatePublic(ks);
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException();
    }
  }

  /**
   * Generates private key from given key bytes
   *
   * @param key Private key which is according to pkcs8 standard
   * @param algorithm the name of the requested key algorithm
   * @return
   */
  public Key generatePrivateKey(byte[] key, String algorithm)
  {
    byte[] decodedKey = DatatypeConverter.parseBase64Binary(new String(key));
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
    try {
      return KeyFactory.getInstance(algorithm).generatePrivate(keySpec);
    } catch (InvalidKeySpecException e) {
      e.printStackTrace();
      throw new RuntimeException();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException();
    }
  }
}
