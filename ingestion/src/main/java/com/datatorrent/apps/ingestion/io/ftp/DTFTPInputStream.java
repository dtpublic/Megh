package com.datatorrent.apps.ingestion.io.ftp;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPException;

/**
 * DTFTPInputStream is the copy of FTPInputStream and has parallel read fix
 */
public class DTFTPInputStream extends FSInputStream
{
  InputStream wrappedStream;
  FTPClient client;
  FileSystem.Statistics stats;
  boolean closed;
  long pos;

  public DTFTPInputStream(InputStream stream, FTPClient client,
      FileSystem.Statistics stats) {
    if (stream == null) {
      throw new IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException
  {
    return pos;
  }

  // We don't support seek.
  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("Seek not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Seek not supported");
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  /**
   * Close the stream and the connection to FTP Server.
   * Has the following fix:
   * https://issues.apache.org/jira/browse/HADOOP-11136
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new FTPException("Client not connected");
    }

    //Stream should be closed before calling the completePendingCommand().
    wrappedStream.close(); // Fix SPOI-6005
    client.completePendingCommand();
    client.logout();
    client.disconnect();
  }

  // Not supported.

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }

}
