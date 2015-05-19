package com.datatorrent.apps.ingestion.io.ftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.ftp.FTPInputStream;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class DTFTPFileSystem extends FTPFileSystem
{

//  @Override
  public FSDataInputStream open(Path file, int bufferSize, long startOffset) throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:open {}:{}", file, startOffset);
    FTPClient client = connect();
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
//    return super.open(file, bufferSize);
  }

  /**
   * Connect to the FTP server using configuration parameters *
   *
   * @return An FTPClient instance
   * @throws IOException
   */
  private FTPClient connect() throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:connect");
    FTPClient client;
    Configuration conf = getConf();
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    client = new FTPClient();
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new IOException("Server - " + host
                            + " refused connection on port - " + port);
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

    return client;
  }

  /**
   * Resolve against given working directory. *
   *
   * @param workDir
   * @param path
   * @return
   */
  private Path makeAbsolute(Path workDir, Path path)
  {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workDir, path);
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  @SuppressWarnings("deprecation")
  private FileStatus getFileStatus(FTPClient client, Path file)
          throws IOException
  {
    FileStatus fileStat = null;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root dir
      long length = -1; // Length of root dir on server not known
      boolean isDir = true;
      int blockReplication = 1;
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      long modTime = -1; // Modification time of root dir not known.
      Path root = new Path("/");
      return new FileStatus(length, isDir, blockReplication, blockSize,
                            modTime, root.makeQualified(this));
    }
    String pathName = parentPath.toUri().getPath();
    FTPFile[] ftpFiles = client.listFiles(pathName);
    if (ftpFiles != null) {
      for (FTPFile ftpFile: ftpFiles) {
        if (ftpFile.getName().equals(file.getName())) { // file found in dir
          fileStat = getFileStatus(ftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
    }
    else {
      throw new FileNotFoundException("File " + file + " does not exist.");
    }
    return fileStat;
  }

  /**
   * Logout and disconnect the given FTPClient. *
   *
   * @param client
   * @throws IOException
   */
  private void disconnect(FTPClient client) throws IOException
  {
    LOGGER.debug("DTFTPFileSystem:disconnect.");
    if (client != null) {
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
  private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath)
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
    return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
                          accessTime, permission, user, group, filePath.makeQualified(this));
  }

  private FsPermission getPermissions(FTPFile ftpFile)
  {
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new FsPermission(user, group, others);
  }

  private FsAction getFsAction(int accessGroup, FTPFile ftpFile)
  {
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DTFTPFileSystem.class);

}
