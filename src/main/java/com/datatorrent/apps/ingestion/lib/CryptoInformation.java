package com.datatorrent.apps.ingestion.lib;

import java.security.Key;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

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
