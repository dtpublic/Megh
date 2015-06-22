package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class FTPFileMerger extends IngestionFileMerger
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
  
  /* (non-Javadoc)
   * @see com.datatorrent.apps.ingestion.io.output.IngestionFileMerger#getOutputStream(org.apache.hadoop.fs.Path)
   */
  @Override
  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    Path relativePath = new Path(StringUtils.stripStart(partFilePath.toUri().getPath(), "/"));
    return super.getOutputStream(relativePath);
  }

  @Override
  protected void moveToFinalFile(Path source, Path destination) throws IOException
  {
    Path src = new Path(StringUtils.stripStart(source.toUri().getPath(), "/"));
    Path dst = new Path(StringUtils.stripStart(destination.toUri().getPath(), "/"));
    super.moveToFinalFile(src, dst);
  }
}
