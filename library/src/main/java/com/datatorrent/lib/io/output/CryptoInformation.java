package com.datatorrent.lib.io.output;

import java.security.Key;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * <p>CryptoInformation class.</p>
 *
 * @since 1.0.0
 */
public class CryptoInformation
{
  private String transformation;
  @Bind(JavaSerializer.class)
  private Key secretKey;

  private CryptoInformation()
  {

  }

  public CryptoInformation(String transformation, Key secretKey)
  {
    this.secretKey = secretKey;
    this.transformation = transformation;
  }

  public Key getSecretKey()
  {
    return secretKey;
  }

  public String getTransformation()
  {
    return transformation;
  }

}
