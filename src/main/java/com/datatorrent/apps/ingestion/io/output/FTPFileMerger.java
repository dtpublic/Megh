package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class FTPFileMerger extends FileMerger
{
  @Override
  protected FileSystem getOutputFSInstance() throws IOException
  {
    Configuration conf = new Configuration();
    FileSystem fileSystem = new FTPFileSystem();

    URI inputURI = URI.create(filePath);
    String uriWithoutPath = filePath.replaceAll(inputURI.getPath(), "");
    fileSystem.initialize(URI.create(uriWithoutPath), conf);
    return fileSystem;
  }

  @Override
  protected FSDataOutputStream getOutputFSOutStream(Path partFilePath) throws IOException
  {
    return outputFS.create(new Path(StringUtils.stripStart(partFilePath.toUri().getPath(), "/")));
  }

  @Override
  protected void moveFile(Path source, Path destination) throws IOException
  {
    Path src = new Path(StringUtils.stripStart(source.toUri().getPath(), "/"));
    Path dst = new Path(StringUtils.stripStart(destination.toUri().getPath(), "/"));
    super.moveFile(src, dst);
  }
}
