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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.ftp.FTPInputStream;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

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
   * Get FileStatus from given Path object from FTP Server
   *
   * @param file  File for which properties needs to be found
   * @return A FileStatus instance
   * @throws IOException
   */
  public FileStatus getFileStatus(Path file) throws IOException {
    FTPClient client = connect();

    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(client, file);
    } finally {
      this.disconnect(client);
    }

    return fileStatus;
  }

  /**
   * Get status of all the files/folders inside given file object if the Path instance is Directory.
   * If Path is not a directory gives array of FileStatus containing only one element for status of give file.
   *
   * @param file Path object of Directory/File
   * @return Array of FileStatus instances
   * @throws IOException
   */
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
        FileStatus[] fileStats = new FileStatus[ftpFiles.length];

        for(int i = 0; i < ftpFiles.length; ++i) {
          fileStats[i] = getFileStatus(ftpFiles[i], absolute);
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
    client = new DTFTPClient();
    client.setListHiddenFiles(true);
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
    String link = ftpFile.getLink();
    if (link == null) {
      return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime, permission, user, group, filePath.makeQualified(this));
    }
    else {
      return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime, permission, user, group, new Path(link), filePath.makeQualified(this));
    }
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

  public static class DTFTPClient extends FTPClient
  {
    @Override public String printWorkingDirectory() throws IOException
    {
      String workDir = super.printWorkingDirectory();
      return workDir.replaceAll("^\"|\"$", "");
    }
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean exists(FTPClient client, Path file) {
    try {
      return getFileStatus(client, file) != null;
    } catch (FileNotFoundException fnfe) {
      return false;
    } catch (IOException ioe) {
      throw new FTPException("Failed to get file status", ioe);
    }
  }
  
  /** @deprecated Use delete(Path, boolean) instead */
  @Deprecated
  private boolean delete(FTPClient client, Path file) throws IOException {
    return delete(client, file, false);
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean delete(FTPClient client, Path file, boolean recursive)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.toUri().getPath();
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return client.deleteFile(pathName);
    }
    FileStatus[] dirEntries = listStatus(client, absolute);
    if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
      throw new IOException("Directory: " + file + " is not empty.");
    }
    if (dirEntries != null) {
      for (int i = 0; i < dirEntries.length; i++) {
        delete(client, new Path(absolute, dirEntries[i].getPath()), recursive);
      }
    }
    return client.removeDirectory(pathName);
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private FileStatus[] listStatus(FTPClient client, Path file)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return new FileStatus[] { fileStat };
    }
    FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
    FileStatus[] fileStats = new FileStatus[ftpFiles.length];
    for (int i = 0; i < ftpFiles.length; i++) {
      fileStats[i] = getFileStatus(ftpFiles[i], absolute);
    }
    return fileStats;
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean mkdirs(FTPClient client, Path file, FsPermission permission)
      throws IOException {
    boolean created = true;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      Path parent = absolute.getParent();
      created = (parent == null || mkdirs(client, parent, FsPermission
          .getDirDefault()));
      if (created) {
        String parentDir = parent.toUri().getPath();
        client.changeWorkingDirectory(parentDir);
        created = created && client.makeDirectory(pathName);
      }
    } else if (isFile(client, absolute)) {
      throw new IOException(String.format(
          "Can't make directory for path %s since it is a file.", absolute));
    }
    return created;
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private boolean isFile(FTPClient client, Path file) {
    try {
      return getFileStatus(client, file).isFile();
    } catch (FileNotFoundException e) {
      return false; // file does not exist
    } catch (IOException ioe) {
      throw new FTPException("File check failed", ioe);
    }
  }
  
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    final FTPClient client = connect();
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    if (exists(client, file)) {
      if (overwrite) {
        delete(client, file);
      } else {
        disconnect(client);
        throw new IOException("File already exists: " + file);
      }
    }
    
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDirDefault())) {
      parent = (parent == null) ? new Path("/") : parent;
      disconnect(client);
      throw new IOException("create(): Mkdirs failed to create: " + parent);
    }
    client.allocate(bufferSize);
    // Change to parent directory on the server. Only then can we write to the
    // file on the server by opening up an OutputStream. As a side effect the
    // working directory on the server is changed to the parent directory of the
    // file. The FTP client connection is closed when close() is called on the
    // FSDataOutputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    FSDataOutputStream fos = new FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!client.isConnected()) {
          throw new FTPException("Client not connected");
        }
        boolean cmdCompleted = client.completePendingCommand();
        disconnect(client);
        if (!cmdCompleted) {
          throw new FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
        }
      }
    };
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fos.close();
      throw new IOException("Unable to create file: " + file + ", Aborting");
    }
    return fos;
  }
  
  /*
   * Assuming that parent of both source and destination is the same. Is the
   * assumption correct or it is suppose to work like 'move' ?
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = rename(client, src, dst);
      return success;
    } finally {
      disconnect(client);
    }
  }
  
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   * 
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   */
  private boolean rename(FTPClient client, Path src, Path dst)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absoluteSrc = makeAbsolute(workDir, src);
    Path absoluteDst = makeAbsolute(workDir, dst);
    if (!exists(client, absoluteSrc)) {
      throw new IOException("Source path " + src + " does not exist");
    }
    if (exists(client, absoluteDst)) {
      throw new IOException("Destination path " + dst
          + " already exist, cannot rename!");
    }
    String parentSrc = absoluteSrc.getParent().toUri().toString();
    String parentDst = absoluteDst.getParent().toUri().toString();
    String from = src.getName();
    String to = dst.getName();
    if (!parentSrc.equals(parentDst)) {
      throw new IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
    }
    client.changeWorkingDirectory(parentSrc);
    boolean renamed = client.rename(from, to);
    return renamed;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DTFTPFileSystem.class);

}
