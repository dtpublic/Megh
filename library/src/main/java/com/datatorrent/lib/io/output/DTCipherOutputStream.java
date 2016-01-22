/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.output;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

/**
 * This class represents wrapper over FilterOutputStream to handle encrypted
 * data, meta data
 */
public class DTCipherOutputStream extends FilterOutputStream
{
  private ObjectOutputStream encryptedOutputStream;

  public DTCipherOutputStream(OutputStream outputStream, Cipher cipher, EncryptionMetaData medatata) throws IOException
  {
    super(outputStream);
    writeMetadataToFile(outputStream, medatata);
    encryptedOutputStream = new ObjectOutputStream(new CipherOutputStream(outputStream, cipher));
  }

  private void writeMetadataToFile(OutputStream outputStream, EncryptionMetaData metaData)
  {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(outputStream);
      oos.writeObject(metaData);
      oos.flush();
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  @Override
  public void write(byte[] b) throws IOException
  {
    encryptedOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    encryptedOutputStream.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException
  {
    encryptedOutputStream.write(b);
  }

  @Override
  public void flush() throws IOException
  {
    encryptedOutputStream.flush();
  }

  @Override
  public void close() throws IOException
  {
    encryptedOutputStream.close();
  }
}
