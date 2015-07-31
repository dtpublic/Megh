package com.datatorrent.apps.ingestion.io.ftp;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.Application.Scheme;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.malhar.lib.io.block.BlockMetadata;
import com.datatorrent.malhar.lib.io.block.ReaderContext;
import com.google.common.base.Preconditions;

//@StatsListener.DataQueueSize
/**
 * <p>FTPBlockReader class.</p>
 *
 * @since 1.0.0
 */
public class FTPBlockReader extends BlockReader
{
  private String host;

  private int port;

  private String userName;

  private String password;

  public FTPBlockReader()
  {
    super(Scheme.FTP);
    this.readerContext = new FTPBlockReaderContext();
    port = FTP.DEFAULT_PORT;
    userName = "anonymous";
    password = "guest";
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    Preconditions.checkArgument(uri != null || host != null, "missing uri or host");

    DTFTPFileSystem fileSystem = new DTFTPFileSystem();
    if (uri != null) {
      URI inputURI = URI.create(uri);
      String uriWithoutPath = uri.replaceAll(inputURI.getPath(), "");
      fileSystem.initialize(URI.create(uriWithoutPath), configuration);
    }
    else {
      String ftpUri = "ftp://" + userName + ":" + password + "@" + host + ":" + port;
      LOG.debug("ftp uri {}", ftpUri);
      fileSystem.initialize(URI.create(ftpUri), configuration);
    }
    return fileSystem;
  }

  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    LOG.debug("FTPBlockReader:initReaderFor: {}", block.getFilePath());
    if(fs == null){
      fs = getFSInstance();
    }
    return ((DTFTPFileSystem) fs).open(new Path(block.getFilePath()), 4096, block.getOffset());
  }

  private static final Logger LOG = LoggerFactory.getLogger(FTPBlockReader.class);

  /**
   * BlockReadeContext for reading FTP Blocks.<br/>
   * This should use read API without offset.
   */
  private static class FTPBlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int) (blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      // If we use read call with offset then it will try to seek on FTP stream and throw exception
      // Hence use read call without offset
      // Offset handling is done using setRestartOffset in DTFTPFileSystem.java
      stream.readFully(record, 0, bytesToRead);
      entity.setUsedBytes(bytesToRead);
      entity.setRecord(record);

      return entity;
    }
  }

  /**
   * Sets the ftp server host.
   *
   * @param host
   */
  public void setHost(String host)
  {
    this.host = host;
  }

  /**
   * @return the ftp server host.
   */
  public String getHost()
  {
    return host;
  }

  /**
   * Sets the ftp server port
   *
   * @param port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the ftp server port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Sets the user name which is used for login to the server.
   *
   * @param userName
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * @return the user name
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * Sets the password which is used for login to the server.
   *
   * @param password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * @return the password
   */
  public String getPassword()
  {
    return password;
  }
}
