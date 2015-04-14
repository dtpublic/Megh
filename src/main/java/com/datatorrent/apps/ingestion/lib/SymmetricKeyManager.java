package com.datatorrent.apps.ingestion.lib;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates symmetric keys for crypto
 * 
 * @author Priyanka
 */
public class SymmetricKeyManager
{
  private static final Logger LOG = LoggerFactory.getLogger(SymmetricKeyManager.class);
  private static final String CRYPTO_PASSWORD = "dtsecretpassword";
  private static final String SECRET_KEY_ALGO = "PBEWithMD5AndDES";
  private static final String SALT_VALUE = "passwordsalt";

  private static final int ITERATION_COUNT = 65536;
  private static final int KEY_LENGTH = 128;

  private static SymmetricKeyManager keyManager = new SymmetricKeyManager();

  private SymmetricKeyManager()
  {

  }

  public static SymmetricKeyManager getInstance()
  {
    return keyManager;
  }

  public SecretKey generateSymmetricKeyForAES()
  {
    return generateSymmetricKeyForAES(CRYPTO_PASSWORD, KEY_LENGTH);
  }

  public SecretKey generateSymmetricKeyForAES(String password, int keyLength)
  {
    try {
      SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGO);
      KeySpec keySpec = new PBEKeySpec(password.toCharArray(), SALT_VALUE.getBytes("UTF-8"), ITERATION_COUNT, keyLength);
      SecretKey key = factory.generateSecret(keySpec);
      SecretKey secret = new SecretKeySpec(key.getEncoded(), "AES");
      return secret;
    } catch (InvalidKeySpecException e) {
      throw new RuntimeException(e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Selected algorithm " + SECRET_KEY_ALGO + " not supported by crypto provider.");
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
