/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.malhar.lib.io.fs.FilterStreamCodec.CipherFilterStreamContext;
import com.datatorrent.malhar.lib.io.fs.FilterStreamCodec.GZIPFilterStreamContext;
import com.datatorrent.malhar.lib.io.fs.FilterStreamCodec.GZipFilterStreamProvider;
import com.datatorrent.malhar.lib.io.fs.FilterStreamContext;
import com.datatorrent.malhar.lib.io.fs.FilterStreamProvider;

/**
 * Stream providers required for ingestion
 */
public class FilterStreamProviders
{
  /**
   * Wrapper over GZIPOutputStream to measure time taken for compression.
   */
  public static class TimedGZIPOutputStream extends GZIPOutputStream
  {
    //Time taken to compress this output stream
    long timeTakenNano = 0;

    /**
     * @param out
     * @throws IOException
     */
    public TimedGZIPOutputStream(OutputStream out) throws IOException
    {
      super(out);
    }

    /**
     * Calls write on underlying GZIPOutputStream. Records time taken in executing the write call.
     * @see java.util.zip.GZIPOutputStream#write(byte[], int, int)
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
   * Stream context for TimedGZIPOutputStream
   */
  public static class TimedGZIPFilterStreamContext extends GZIPFilterStreamContext
  {
    public TimedGZIPFilterStreamContext(OutputStream outputStream) throws IOException
    {
      super(outputStream);
      filterStream = new TimedGZIPOutputStream(outputStream);
    }
  }

  /**
   * A provider for Timed GZIP filter
   */
  public static class TimedGZipFilterStreamProvider extends GZipFilterStreamProvider
  {
    transient TimedGZIPFilterStreamContext streamContext;

    @Override
    public FilterStreamContext<GZIPOutputStream> getFilterStreamContext(OutputStream outputStream) throws IOException
    {
      streamContext = new TimedGZIPFilterStreamContext(outputStream);
      return streamContext;
    }

    public long getTimeTaken()
    {
      return ((TimedGZIPOutputStream) streamContext.getFilterStream()).getTimeTaken();
    }
  }

  
  /**
   * Wrapper over CipherOutputStream to measure time taken for encryption
   */
  public static class TimedCipherOutputStream extends CipherOutputStream
  {

    long timeTakenNano = 0;
    
    public TimedCipherOutputStream(OutputStream os, Cipher cipher)
    {
      super(os, cipher);
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
   * Filter StreamContext for TimedCipherOutputStream
   * @see CipherFilterStreamContext
   */
  public static class TimedCipherFilterStreamContext extends CipherFilterStreamContext
  {
    public TimedCipherFilterStreamContext(OutputStream outputStream, Cipher cipher) throws IOException
    {
      super(outputStream, cipher);
      filterStream = new TimedCipherOutputStream(outputStream, cipher);
    }
  }
  
  
  public static class TimedCipherStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<CipherOutputStream, OutputStream>
  {
    transient TimedCipherFilterStreamContext streamContext;
    private Key secretKey;
    private String transformation;

    public TimedCipherStreamProvider(String transformation, Key key)
    {
      this.transformation = transformation;
      secretKey = key;
    }

    @Override
    protected FilterStreamContext<CipherOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      CipherProvider cryptoProvider = new CipherProvider(transformation);
      Cipher cipher = cryptoProvider.getEncryptionCipher(secretKey);
      streamContext = new TimedCipherFilterStreamContext(outputStream, cipher);
      return streamContext;
    }
    

    public long getTimeTaken()
    {
      return ((TimedCipherOutputStream) streamContext.getFilterStream()).getTimeTaken();
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(FilterStreamProviders.class);
}
