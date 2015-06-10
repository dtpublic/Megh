package com.datatorrent.apps.ingestion.process;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class LzoOutputStream extends FilterOutputStream
{
  /**
   * Creates an output stream filter built on top of the specified
   * underlying output stream.
   *
   * @param out the underlying output stream to be assigned to
   *            the field <tt>this.out</tt> for later use, or
   *            <code>null</code> if this instance is to be
   *            created without an underlying stream.
   */
  public LzoOutputStream(OutputStream out)
  {
    super(out);
  }

  /**
   * Finishes writing compressed data to the output stream without closing
   * the underlying stream. Use this method when applying multiple filters
   * in succession to the same output stream.
   * @exception IOException if an I/O error has occurred
   */
  public abstract void finish() throws IOException;
  
  //Time taken to compress this output stream
  private long timeTakenNano = 0;
  
  /**
   * Calls write on underlying FilterOutputStream. Records time taken in executing the write call.
   * @see java.io.FilterOutputStream#write(byte[], int, int)
    
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
  
  public long getTimeTaken(){
    return timeTakenNano/1000;
  }

}
