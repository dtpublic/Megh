package com.datatorrent.contrib.hdht;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.netlet.util.Slice;


public class PurgeTest
{
  Slice newSlice(int i) {
    return new Slice(ByteBuffer.allocate(4).putInt(i).array());
  }

  byte[] newData(int i) {
    return ("data" + new Integer(i).toString()).getBytes();
  }

  @Test
  public void testCacheWithPurge()
  {
    List<byte[]> dataList = Lists.newArrayList();
    byte[] deleted = HDHTWriter.DELETED;
    WriteCache cache = new WriteCache(new HDHTWriter.DefaultKeyComparator());
    for(int i = 0; i < 10; i++) {
      Slice s = new Slice(ByteBuffer.allocate(4).putInt(i).array());
      byte[] data = ByteBuffer.allocate(4).putInt(i).array();
      dataList.add(data);
      cache.put(s, data);
    }
    Slice start = new Slice(ByteBuffer.allocate(4).putInt(2).array());
    Slice end = new Slice(ByteBuffer.allocate(4).putInt(5).array());

    Assert.assertEquals("Number of element in cache", 10, cache.size());
    cache.purge(start, end);
    Assert.assertEquals("Number of element in cache after purge", 6, cache.size());

    // data available after purge.
    byte[] data =cache.get(new Slice(ByteBuffer.allocate(4).putInt(1).array()));
    Assert.assertEquals("Byte data ", data, dataList.get(1));

    // data at start of purge list
    data =cache.get(new Slice(ByteBuffer.allocate(4).putInt(2).array()));
    Assert.assertEquals("Byte data ", data, HDHTWriter.DELETED);

    // data in between purge list
    data =cache.get(new Slice(ByteBuffer.allocate(4).putInt(3).array()));
    Assert.assertEquals("Byte data ", data, HDHTWriter.DELETED);

    // data at end of purge list.
    data =cache.get(new Slice(ByteBuffer.allocate(4).putInt(5).array()));
    Assert.assertEquals("Byte data ", data, HDHTWriter.DELETED);

    // just after purge end.
    data =cache.get(new Slice(ByteBuffer.allocate(4).putInt(6).array()));
    Assert.assertEquals("Byte data ", data, dataList.get(6));

    // data not available should return null
    data = cache.get(new Slice(ByteBuffer.allocate(4).putInt(10).array()));
    Assert.assertEquals("Invalid data", data, null);

    /**
     * Add a new data in the purge list
     * get on newly added data should return valid data.
     */
    cache.put(new Slice(ByteBuffer.allocate(4).putInt(3).array()), dataList.get(3));
    data = cache.get(new Slice(ByteBuffer.allocate(4).putInt(3).array()));
    Assert.assertEquals("Data added after purge is available", data, dataList.get(3));
  }

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  /**
   * Test purged data is handled correctly in committed cache.
   */
  @Test
  public void testPurgeWithCommittedCache() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush

    hds.beginWindow(1);
    for(int i = 0; i < 10; i++) {
      hds.put(1, newSlice(i), newData(i));
    }
    hds.purge(1, newSlice(2), newSlice(5));
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    hds.put(1, newSlice(3), newData(3));
    for(int i = 0; i < 10; i++) {
      hds.put(1, newSlice(i + 10), newData(i + 10));
    }
    hds.endWindow();
    hds.checkpointed(2);


    byte[] data = hds.getUncommitted(1, newSlice(3));
    Assert.assertEquals("data purged and added in next committed window ", new String(data), new String(newData(3)));

    data = hds.getUncommitted(1, newSlice(2));
    Assert.assertEquals("data purged and added in next committed window ", data, null);

    data = hds.getUncommitted(1, newSlice(7));
    Assert.assertEquals("Data from old committed window ", new String(data), new String(newData(7)));

    data = hds.getUncommitted(1, newSlice(13));
    Assert.assertEquals("data purged and added in next committed window ", new String(data), new String(newData(13)));

    hds.beginWindow(3);
    hds.purge(1, newSlice(2), newSlice(10));
    for(int i = 0; i < 10; i++) {
      hds.put(1, newSlice(i + 20), newData(i + 20));
    }
    hds.endWindow();

    data = hds.getUncommitted(1, newSlice(3));
    Assert.assertEquals("data purged and added in next committed window ", data, null);

    hds.checkpointed(3);
    hds.committed(3);

    data = hds.get(1, newSlice(3));
    Assert.assertEquals("data purged and added in next committed window ", data, null);

    data = hds.get(1, newSlice(1));
    Assert.assertEquals("data purged and added in next committed window ", new String(data), new String(newData(1)));

    data = hds.get(1, newSlice(20));
    Assert.assertEquals("data purged and added in next committed window ", new String(data), new String(newData(20)));

    hds.beginWindow(4);
    hds.purge(1, newSlice(10), newSlice(20));
    hds.endWindow();
    hds.checkpointed(4);
    hds.committed(4);

    data = hds.get(1, newSlice(17));
    Assert.assertEquals("data purged from files", data, null);
  }

  /**
   * Test purged data is removed from data files.
   */
  @Test
  public void testPurgeFromDataFiles() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = newSlice(1);
    String data = "data1";

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush
    hds.beginWindow(1);
    for(int i = 0; i < 10; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.purge(1, newSlice(2), newSlice(5));
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);

    hds.beginWindow(2);
    hds.purge(1, newSlice(0), newSlice(20));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    HDHTReader.BucketMeta meta = hds.loadBucketMeta(1);
    Assert.assertEquals("All data files removed ", 0, meta.files.size());

  }


  /**
   * Test purged data is removed from old files.
   * set file size to 1 M,
   * put data till 3 M
   * call purge for first 1.5M of data
   * see that data files are removed , appropriately.
   */
  @Test
  public void testPurgeOldData() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = newSlice(1);
    String data = "data1";

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setMaxFileSize(1024 * 1024);
    hds.setFlushIntervalCount(0);

    /**
     * Max file size is 1M. Each file contains 58255 records.
     */
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush
    hds.beginWindow(1);
    for(int i = 0; i < 1024 * 1024; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);

    HDHTReader.BucketMeta meta = hds.loadBucketMeta(1);
    int origNumberOfFiles = meta.files.size();
    List<HDHTReader.BucketFileMeta> list = new ArrayList<HDHTReader.BucketFileMeta>(meta.files.values());
    HDHTReader.BucketFileMeta fmeta2 = list.get(2);

    hds.beginWindow(2);


    int start = sliceToInt(fmeta2.startKey);
    int end = sliceToInt(hds.getEndKey(1, fmeta2, null)); end--;
    int mid = start + (end - start) / 2;

    hds.purge(1, newSlice(0), newSlice(mid));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    meta = hds.loadBucketMeta(1);
    HDHTReader.BucketFileMeta fmeta = meta.files.values().iterator().next();
    Assert.assertEquals("Name of new file is 1-18", fmeta.name, "1-18");
    Assert.assertEquals("Number of files after purge is ", meta.files.size(), origNumberOfFiles - 2);
    Assert.assertEquals("Keys present in partial file ", (end - mid), numberOfKeys(fa, 1, meta.files.values().iterator().next().name));


    /** purge of same key does not perform any data write */
    hds.beginWindow(3);
    hds.purge(1, newSlice(0), newSlice(1024));
    hds.endWindow();
    hds.checkpointed(3);
    hds.committed(3);

    meta = hds.loadBucketMeta(1);
    Assert.assertEquals("No file deleted or added ", meta.files.size(), origNumberOfFiles - 2);
    Assert.assertEquals("Number of files after purge is ", meta.files.size(), origNumberOfFiles - 2);
    Assert.assertEquals("Keys present in partial file ", (end - mid), numberOfKeys(fa, 1, meta.files.values().iterator().next().name));
    printMeta(hds, meta);

    /** purge at middle of the range */
    Assert.assertEquals("File does not contain data for file 5 ", true, meta.files.containsKey(list.get(5).startKey));
    hds.beginWindow(4);
    start = sliceToInt(list.get(5).startKey) - 1;
    end = sliceToInt(hds.getEndKey(1, list.get(5), null)) + 1;
    hds.purge(1, newSlice(start), newSlice(end));
    logger.debug("Purging key range {} {}", start, end);
    hds.endWindow();
    hds.checkpointed(4);
    hds.committed(4);

    meta = hds.loadBucketMeta(1);
    printMeta(hds, meta);
    Assert.assertEquals("File does not contain data for file 5 ", false, meta.files.containsKey(list.get(5).startKey));
    Assert.assertEquals("No file deleted or added ", meta.files.size(), origNumberOfFiles - 3);
    Assert.assertEquals("Next file starts from 1 less key ", true, meta.files.containsKey(newSlice(end+1)));
  }

  long numberOfKeys(HDHTFileAccess fa, long bucketId, String name) throws IOException
  {
    HDHTFileAccess.HDSFileReader reader = fa.getReader(bucketId, name);
    Slice key = new Slice(null, 0, 0);
    Slice value = new Slice(null, 0, 0);
    long seq = 0;
    while (reader.next(key, value)) {
      seq++;
    }
    return seq;
  }

  int sliceToInt(Slice s) {
    ByteBuffer bb = ByteBuffer.wrap(s.buffer, s.offset, s.length);
    return bb.getInt();
  }

  /**
   * Test purged data is removed from data files.
   */
  @Test
  public void testDeleteDataFiles() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = newSlice(1);
    String data = "data1";

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush
    hds.beginWindow(1);
    for(int i = 0; i < 10; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.purge(1, newSlice(2), newSlice(5));
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);

    HDHTReader.BucketMeta meta = hds.loadBucketMeta(1);
    Assert.assertEquals("All data files removed ", 1, meta.files.size());

    hds.beginWindow(2);
    hds.delete(1, newSlice(0));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    meta = hds.loadBucketMeta(1);
    Assert.assertEquals("All data files removed ", 1, meta.files.size());

  }

  private void printMeta(HDHTWriter hds, HDHTReader.BucketMeta meta) throws IOException
  {
    logger.debug("Number of files {}", meta.files.size());
    for(HDHTReader.BucketFileMeta fmeta : meta.files.values()) {
      int start = sliceToInt(fmeta.startKey);
      int end = sliceToInt(hds.getEndKey(1, fmeta, null));
      logger.debug("File {} start {} end {}", fmeta.name, start, end);
    }
  }

  /**
   * checkpointed(1)  1 -> 10
   * checkpointed(2)  1 -> 20
   * checkpointed(3)  1 -> 30
   * checkpointed(4)  1 -> 40
   * committed(2)
   * checkpointed(5)
   *
   * restore from 3rd checkpoint.
   * do a get and value should be 30.
   */
  @Test
  public void testWalRecoveryValues() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = newSlice(1);
    String data = "data1";

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);
    hds.setup(null);
    ((MockFileAccess)fa).disableChecksum();

    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush

    hds.beginWindow(1);
    for(int i = 0; i < 10; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    for(int i = 10; i < 20; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.endWindow();
    hds.checkpointed(2);

    hds.beginWindow(3);
    for(int i = 20; i < 30; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.purge(1, newSlice(22), newSlice(25));
    hds.purge(1, newSlice(12), newSlice(15));
    hds.endWindow();
    hds.checkpointed(3);

    // Commit window id 2
    hds.committed(2);
    // use checkpoint after window 3 for recovery.
    HDHTWriter newOperator = TestUtils.clone(new Kryo(), hds);

    hds.beginWindow(4);
    for(int i = 30; i < 40; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.purge(1, newSlice(12), newSlice(15));
    hds.endWindow();
    hds.checkpointed(4);

    hds.beginWindow(5);
    for(int i = 40; i < 50; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.purge(1, newSlice(31), newSlice(35));
    hds.endWindow();
    hds.checkpointed(5);
    hds.forceWal();

    /* Simulate recovery after failure, checkpoint is restored to after
       processing of window 3.
     */
    newOperator.setFlushIntervalCount(1);
    newOperator.setFileStore(fa);
    newOperator.setFlushSize(1);
    newOperator.setup(null);
    newOperator.writeExecutor = MoreExecutors.sameThreadExecutor();

    // This should run recovery, as first tuple is added in bucket
    newOperator.beginWindow(4);
    newOperator.put(1, newSlice(1), newData(1));
    Assert.assertEquals("Tuples in write cache ", 1, newOperator.unflushedDataSize(1));
    Assert.assertEquals("Tuples in committed cache", 6, newOperator.committedDataSize(1));
    newOperator.endWindow();
    newOperator.checkpointed(4);

    Assert.assertEquals("No data for purged keys", null, newOperator.getUncommitted(1, newSlice(24)));
    Assert.assertEquals("No data for purged keys", null, newOperator.getUncommitted(1, newSlice(14)));
    Assert.assertArrayEquals("Data for valid keys", newData(21), newOperator.getUncommitted(1, newSlice(21)));

    Assert.assertArrayEquals("Purged data present in files before committed", newData(14), newOperator.get(1, newSlice(14)));
    newOperator.committed(3);
    Assert.assertEquals("Purged data removed from files after committed", null, newOperator.get(1, newSlice(14)));
  }


  /**
   * Purge data from start of the file.
   */
  @Test
  public void testMultiplePurgeFromDataFiles() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);
    hds.setup(null);

    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush
    hds.beginWindow(1);
    for(int i = 100; i < 1000; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);

    HDHTReader.BucketMeta meta = hds.loadBucketMeta(1);
    HDHTReader.BucketFileMeta fmeta = meta.files.firstEntry().getValue();

    hds.beginWindow(2);
    hds.purge(1, newSlice(0), newSlice(150));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    meta = hds.loadBucketMeta(1);
    fmeta = meta.files.firstEntry().getValue();
    TreeMap<Slice, byte[]> data = getData(fa, 1, fmeta.name);
    int startKey = sliceToInt(data.firstKey());
    Assert.assertEquals("The start key in new file", 151, startKey);
    int endKey = sliceToInt(data.lastKey());
    Assert.assertEquals("The end key in neww file", 999, endKey);
  }

  /**
   * Purge data from start, middle and end of the file.
   */
  @Test
  public void purgeDataFromMiddleOfFile() throws IOException
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key
    hds.setFlushIntervalCount(0);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush

    hds.beginWindow(1);
    for(int i = 100; i < 1000; i++)
      hds.put(1, newSlice(i), newData(i));
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);

    hds.beginWindow(2);
    hds.purge(1, newSlice(150), newSlice(250));
    hds.purge(1, newSlice(200), newSlice(400));
    hds.purge(1, newSlice(450), newSlice(700));
    hds.purge(1, newSlice(950), newSlice(1500));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    HDHTReader.BucketFileMeta fmeta = hds.loadBucketMeta(1).files.firstEntry().getValue();
    TreeMap<Slice, byte[]> data = getData(fa, 1, fmeta.name);
    int startKey = sliceToInt(data.firstKey());
    Assert.assertEquals("The start key in new file", 100, startKey);
    int endKey = sliceToInt(data.lastKey());

    Assert.assertArrayEquals("Key 149 is present int file ", newData(149), data.get(newSlice(149)));
    Assert.assertEquals("Key 150 is removed from file ", null, data.get(newSlice(150)));
    Assert.assertEquals("Key 160 is removed from file ", null, data.get(newSlice(160)));
    Assert.assertEquals("Key 220 is removed from file ", null, data.get(newSlice(220)));
    Assert.assertEquals("Key 400 is removed from file ", null, data.get(newSlice(400)));
    Assert.assertArrayEquals("Key 401 is present int file ", newData(401), data.get(newSlice(401)));

    Assert.assertArrayEquals("Key 449 is present int file ", newData(449), data.get(newSlice(449)));
    Assert.assertEquals("Key 450 is removed from file ", null, data.get(newSlice(450)));
    Assert.assertEquals("Key 500 is removed from file ", null, data.get(newSlice(500)));
    Assert.assertEquals("Key 700 is removed from file ", null, data.get(newSlice(700)));
    Assert.assertArrayEquals("Key 701 is present int file ", newData(701), data.get(newSlice(701)));

    Assert.assertArrayEquals("Key 949 is present int file ", newData(949), data.get(newSlice(949)));
    Assert.assertEquals("Key 950 is removed from file ", null, data.get(newSlice(950)));
    Assert.assertEquals("Key 999 is removed from file ", null, data.get(newSlice(999)));

    Assert.assertEquals("The end key in new file", 949, endKey);
  }

  TreeMap<Slice, byte[]> getData(HDHTFileAccess fa, long bucketId, String name) throws IOException
  {
    HDHTFileAccess.HDSFileReader reader = fa.getReader(bucketId, name);

    TreeMap<Slice, byte[]> data = new TreeMap<>(new HDHTReader.DefaultKeyComparator());
    reader.readFully(data);
    return data;
  }

  private static final Logger logger = LoggerFactory.getLogger(PurgeTest.class);
}
