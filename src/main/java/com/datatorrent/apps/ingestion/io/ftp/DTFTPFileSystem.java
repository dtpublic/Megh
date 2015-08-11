package com.datatorrent.apps.ingestion.io.ftp;

import java.io.IOException;
import java.io.InputStream;

import java.net.ConnectException;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.fs.ftp.FTPInputStream;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * <p>DTFTPFileSystem class.</p>
 *
 * @since 1.0.0
 */
public class DTFTPFileSystem extends BaseFTPFileSystem
{
  protected String parentDir = null;
  private transient FTPClient client = null;
  private boolean reuse = false;
  private static final String CURRENT_DIRECTORY = ".";
  private static final String PARENT_DIRECTORY = "..";

  public FSDataInputStream open(Path file, int bufferSize, long startOffset) throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:open {}:{}", file, startOffset);
    FTPClient client = createNewFTPClient();
    client.setRestartOffset(startOffset);
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isDirectory()) {
      disconnect(client);
      throw new IOException("Path " + file + " is a directory.");
    }
    client.allocate(bufferSize);
    Path parent = absolute.getParent();
    // Change to parent directory on the
    // server. Only then can we read the
    // file
    // on the server by opening up an InputStream. As a side effect the working
    // directory on the server is changed to the parent directory of the file.
    // The FTP client connection is closed when close() is called on the
    // FSDataInputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    InputStream is = client.retrieveFileStream(file.getName());
    FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is,
                                                                     client, statistics));
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fis.close();
      throw new IOException("Unable to open file: " + file + ", Aborting");
    }
    return fis;
  }

  /**
   * Get status of all the files/folders inside given file object if the Path instance is Directory.
   * If Path is not a directory gives array of FileStatus containing only one element for status of give file.
   *
   * @param file Path object of Directory/File
   * @return Array of FileStatus instances
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path file) throws IOException {
    FTPClient client = connect();

    try {
      Path workDir = new Path(client.printWorkingDirectory());
      Path absolute = this.makeAbsolute(workDir, file);
      FileStatus fileStat = this.getFileStatus(client, absolute);
      if(fileStat.isFile()) {
        return new FileStatus[]{fileStat};
      } else {
        FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
        FileStatus[] fileStats = new FileStatus[ftpFiles.length - 2];
        int j = 0;
        // Ignoring the . & .. files
        for(int i = 0; i < ftpFiles.length; ++i) {
          if(ftpFiles[i].getName().equals(CURRENT_DIRECTORY) || ftpFiles[i].getName().equals(PARENT_DIRECTORY)) {
            continue;
          }
          fileStats[j++] = getFileStatus(ftpFiles[i], absolute);
        }

        return fileStats;
      }
    }
    finally {
      this.disconnect(client);
    }
  }



  /**
   * Connect to the FTP server using configuration parameters *
   * If reuse is false then only creates new Connection
   * @return An FTPClient instance
   * @throws IOException
   */
  @Override
  protected FTPClient connect() throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:connect");
    if(reuse && client != null) {
      // Some API's changes the working directory & this System maintains the state.
      // So, before doing any action, set the working directory to parent directory.
      client.changeWorkingDirectory(parentDir);
      return client;
    }
    reuse = true;
    client = createNewFTPClient();
    return client;
  }

  /**
   * Creates the new client connection using configuration parameters
   * @return An FTPClient instance
   * @throws IOException
   */
  protected FTPClient createNewFTPClient() throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:connect");
    Configuration conf = getConf();
    String host = conf.get(FS_FTP_HOST);
    int port = conf.getInt(FS_FTP_HOST_PORT, FTP.DEFAULT_PORT);
    String user = conf.get(FS_FTP_USER_PREFIX + host);
    String password = conf.get(FS_FTP_PASSWORD_PREFIX + host);
    FTPClient client = new DTFTPClient();
    client.setListHiddenFiles(true);
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw NetUtils.wrapException(host, port,
          NetUtils.UNKNOWN_HOST, 0,
          new ConnectException("Server response " + reply));
    }
    else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
    }
    else {
      throw new IOException("Login failed on server - " + host + ", port - "
                            + port);
    }
    parentDir = client.printWorkingDirectory();
    return client;
  }

  /**
   * If reuse is false then logout and disconnect the given FTPClient.*
   *
   * @param client
   * @throws IOException
   */
  @Override
  protected void disconnect(FTPClient client) throws IOException
  {

    LOGGER.debug("DTFTPFileSystem:disconnect.");
    if (client != null && !reuse) {
      if (!client.isConnected()) {
        throw new FTPException("Client not connected");
      }
      boolean logoutSuccess = client.logout();
      client.disconnect();
      if (!logoutSuccess) {
        LOGGER.warn("Logout failed while disconnecting, error code - "
                    + client.getReplyCode());
      }
    }
  }

  /**
   * Convert the file information in FTPFile to a {@link FileStatus} object. *
   *
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  @SuppressWarnings("deprecation")
  @Override
  protected FileStatus getFileStatus(FTPFile ftpFile, Path parentPath)
  {
    long length = ftpFile.getSize();
    boolean isDir = ftpFile.isDirectory();
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    long blockSize = DEFAULT_BLOCK_SIZE;
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    long accessTime = 0;
    FsPermission permission = getPermissions(ftpFile);
    String user = ftpFile.getUser();
    String group = ftpFile.getGroup();
    Path filePath = new Path(parentPath, ftpFile.getName());
    String link = ftpFile.getLink();
    if (link == null) {
      return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime, permission, user, group, filePath.makeQualified(this));
    }
    else {
      return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime, permission, user, group, new Path(link), filePath.makeQualified(this));
    }
  }

  public static class DTFTPClient extends FTPClient
  {
    /**
     * Removed the quotes from working directory
     * @return
     * @throws IOException
     */
    @Override public String printWorkingDirectory() throws IOException
    {
      String workDir = super.printWorkingDirectory();
      return workDir.replaceAll("^\"|\"$", "");
    }
  }

  @Override
  public void close()
  {
    reuse = false;
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(DTFTPFileSystem.class);

}
