/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.util.zip.GZIPOutputStream;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.io.output.EncryptionMetaData;
import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.DTCipherOutputStream;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.lib.io.fs.FilterStreamCodec.GZipFilterStreamProvider;
import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;


/**
 * Stream providers required for ingestion
 *
 * @since 1.0.0
 */
public class FilterStreamProviders
{
  /**
   * Wrapper over GZIPOutputStream to measure time taken for compression.
   */
  public static class TimedGZIPOutputStream extends GZIPOutputStream
  {
    MutableLong timeTakenNano;
    /**
     * @param out
     * @param timeTakenNano 
     * @throws IOException
     */
    public TimedGZIPOutputStream(OutputStream out, MutableLong timeTakenNano) throws IOException
    {
      super(out);
      this.timeTakenNano = timeTakenNano;
    }

    /**
     * Calls write on underlying GZIPOutputStream. Records time taken in executing the write call.
     * @see java.util.zip.GZIPOutputStream#write(byte[], int, int)
     */
    @Override
    public synchronized void write(byte[] buffer, int off, int len) throws IOException
    {
      long startTime = System.nanoTime();
      super.write(buffer, off, len);
      long endTime = System.nanoTime();
      timeTakenNano.add(endTime - startTime);
    }
    
  }
  
  
  /**
   * Stream context for TimedGZIPOutputStream
   */
  public static class TimedGZIPFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<GZIPOutputStream>
  {
    public TimedGZIPFilterStreamContext(OutputStream outputStream, MutableLong timeTakenNano) throws IOException
    {
      filterStream = new TimedGZIPOutputStream(outputStream, timeTakenNano);
    }
    
    @Override
    public void finalizeContext() throws IOException
    {
      filterStream.finish();
    }
  }

  /**
   * A provider for Timed GZIP filter
   */
  public static class TimedGZipFilterStreamProvider extends GZipFilterStreamProvider
  {
    MutableLong timeTakenNano;

    /**
     * 
     */
    public TimedGZipFilterStreamProvider()
    {
      timeTakenNano = new MutableLong();
    }
    
    @Override
    public FilterStreamContext<GZIPOutputStream> getFilterStreamContext(OutputStream outputStream) throws IOException
    {
      timeTakenNano = new MutableLong();
      return new TimedGZIPFilterStreamContext(outputStream, timeTakenNano);
    }

    public long getTimeTaken()
    {
      return timeTakenNano.longValue()/1000;
    }
    
    /**
     * @return the timeTakenNano
     */
    public MutableLong getTimeTakenNano()
    {
      return timeTakenNano;
    }
    
    /**
     * @param timeTakenNano the timeTakenNano to set
     */
    public void setTimeTakenNano(MutableLong timeTakenNano)
    {
      this.timeTakenNano = timeTakenNano;
    }
  }

  
  /**
   * Wrapper over CipherOutputStream to measure time taken for encryption
   */
  public static class TimedCipherOutputStream extends DTCipherOutputStream
  {

    long timeTakenNano = 0;

    public TimedCipherOutputStream(OutputStream os, Cipher cipher, EncryptionMetaData metadata) throws IOException
    {
      super(os, cipher, metadata);
    }

    /**
     * Calls write on underlying CipherOutputStream. Records time taken in executing the write call.
     * @see java.util.zip.CipherOutputStream#write(byte[], int, int)
     */
    @Override
    public synchronized void write(byte[] buf, int off, int len) throws IOException
    {
      long startTime = System.nanoTime();
      super.write(buf, off, len);
      long endTime = System.nanoTime();
      
      timeTakenNano += (endTime - startTime);
    }

    /**
     * @return the timeTaken
     */
    public long getTimeTakenNano()
    {
      return timeTakenNano;
    }
    
    /**
     * @return the timeTaken
     */
    public long getTimeTaken()
    {
      return timeTakenNano/1000;
    }

  }

  /**
   * This filter should be used when cipher cannot be reused for example when writing to different output streams
   */
  public static class CipherFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<DTCipherOutputStream>
  {
    public CipherFilterStreamContext(OutputStream outputStream, Cipher cipher, EncryptionMetaData metadata) throws IOException
    {
      filterStream = new DTCipherOutputStream(outputStream, cipher, metadata);
    }
  }

  /**
   * Filter StreamContext for TimedCipherOutputStream
   * @see CipherFilterStreamContext
   */
  public static class TimedCipherFilterStreamContext extends CipherFilterStreamContext
  {
    public TimedCipherFilterStreamContext(OutputStream outputStream, Cipher cipher, EncryptionMetaData metadata) throws IOException
    {
      super(outputStream, cipher, metadata);
    }
  }
  
  
  public static class TimedCipherStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<DTCipherOutputStream, OutputStream>
  {
    transient TimedCipherFilterStreamContext streamContext;
    @Bind(JavaSerializer.class)
    private Key secretKey;
    private String transformation;

    private TimedCipherStreamProvider()
    {

    }

    public TimedCipherStreamProvider(String transformation, Key key)
    {
      this.transformation = transformation;
      secretKey = key;
    }

    @Override
    protected FilterStreamContext<DTCipherOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      EncryptionMetaData metaData = new EncryptionMetaData();
      metaData.setTransformation(transformation);
      Cipher cipher;
      if (isPKI()) {
        Key sessionKey = SymmetricKeyManager.getInstance().generateRandomKey();
        byte[] encryptedSessionKey = encryptSessionkeyWithPKI(sessionKey);
        metaData.setKey(encryptedSessionKey);
        cipher = new CipherProvider(Application.AES_TRANSOFRMATION).getEncryptionCipher(sessionKey);
      } else {
        cipher = new CipherProvider(transformation).getEncryptionCipher(secretKey);
      }
      streamContext = new TimedCipherFilterStreamContext(outputStream, cipher, metaData);
      return streamContext;
    }

    private boolean isPKI()
    {
      if (transformation.equals(Application.RSA_TRANSFORMATION)) {
        return true;
      }
      return false;
    }

    private byte[] encryptSessionkeyWithPKI(Key sessionKey)
    {
      try {
        Cipher rsaCipher = new CipherProvider(transformation).getEncryptionCipher(secretKey);
        return rsaCipher.doFinal(sessionKey.getEncoded());
      } catch (BadPaddingException e) {
        throw new RuntimeException(e);
      } catch (IllegalBlockSizeException e) {
        throw new RuntimeException(e);
      }
    }

    public long getTimeTaken()
    {
      return ((TimedCipherOutputStream) streamContext.getFilterStream()).getTimeTaken();
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(FilterStreamProviders.class);
}
