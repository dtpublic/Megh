package com.datatorrent.apps.ingestion.io.output;

import com.datatorrent.apps.ingestion.io.jms.BytesFileOutputOperator;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class FTPOutputOperator extends BytesFileOutputOperator
{
  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    FileSystem fileSystem = new FTPFileSystem();

    URI inputURI = URI.create(filePath);
    String uriWithoutPath = filePath.replaceAll(inputURI.getPath(), "");
    fileSystem.initialize(URI.create(uriWithoutPath), new Configuration());
    return fileSystem;
  }
}
