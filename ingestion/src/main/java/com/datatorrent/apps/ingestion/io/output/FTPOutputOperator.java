package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.apps.ingestion.io.ftp.BaseFTPFileSystem;
import com.datatorrent.apps.ingestion.io.jms.BytesNonAppendFileOutputOperator;

/**
 * <p>FTPOutputOperator class.</p>
 *
 * @since 1.0.0
 */
public class FTPOutputOperator extends BytesNonAppendFileOutputOperator
{
  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    FileSystem fileSystem = new BaseFTPFileSystem();

    URI inputURI = URI.create(filePath);
    String uriWithoutPath = filePath.replaceAll(inputURI.getPath(), "");
    fileSystem.initialize(URI.create(uriWithoutPath), new Configuration());
    return fileSystem;
  }
}
