package com.datatorrent.apps.ingestion.io.s3;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import com.datatorrent.apps.ingestion.Application;

public class S3BlockReaderTest
{

  String scheme = Application.Schemes.S3N;
  String userKey = "userKey";
  String passKey = "passKey";
  String bucket = "my.s3-bucket";
  String directory = "myDir";

  public static class TestS3BlockReader extends TestWatcher
  {
    public S3BlockReader s3BlockReader;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      s3BlockReader = new S3BlockReader();
    }
  }

  @Rule
  public TestS3BlockReader testS3BlockReader = new TestS3BlockReader();

  @Test
  public void testSanity()
  {
    Assert.assertEquals("Mismatch in bucket name", bucket, testS3BlockReader.s3BlockReader.extractBucket(generateURI()));
  }

  public String generateURI()
  {
    return scheme + "://" + userKey + ":" + passKey + "@" + bucket + "/" + directory + "/";
  }
}
