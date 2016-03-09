/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hdht;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.Min;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.contrib.hdht.HDHTWalManager.PreviousWALDetails;
import com.datatorrent.contrib.hdht.HDHTWalManager.WalPosition;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.fileaccess.FileAccess.FileReader;
import com.datatorrent.lib.fileaccess.FileAccess.FileWriter;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.netlet.util.Slice;

/**
 * Writes data to buckets. Can be sub-classed as operator or used in composite pattern.
 * <p>
 * Changes are accumulated in a write cache and written to a write-ahead-log (WAL). They are then asynchronously flushed
 * to the data files when thresholds for the memory buffer are reached. Changes are flushed to data files at the
 * committed window boundary.
 * <p>
 * When data is read through the same operator (extends reader), full consistency is guaranteed (reads will consider
 * changes that are not flushed). In the event of failure, the operator recovers the write buffer from the WAL.
 *
 * @displayName HDHT Writer
 * @category Output
 * @tags hdht, output operator
 *
 * @since 2.0.0
 */
public class HDHTWriter extends HDHTReader implements CheckpointListener, Operator, HDHT.Writer
{
  private FileAccess walStore;
  private final String WAL_FILES_LOCATION = "/WAL/";
  private final transient HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  private long currentWindowId;
  private final transient HashMap<Long, Bucket> buckets = Maps.newHashMap();
  // After buckets are reorganized on dynamic repartitioning, this map maintains which Wal to look up during recovery for each bucketKey
  public Map<Long, PreviousWALDetails> parentWalMetaDataMap = Maps.newHashMap();
  public HashMap<Long, HDHTWalManager.WalPosition> walPositions = Maps.newLinkedHashMap();
  public HDHTWalManager.WalPosition committedWalPosition;
  public List<PreviousWALDetails> parentWals = new LinkedList<>();
  public Set<PreviousWALDetails> alreadyCopiedWals = new HashSet<>();

  @VisibleForTesting
  protected transient ExecutorService writeExecutor;
  private transient volatile Throwable writerError;
  protected Set<Long> bucketKeys = Sets.newHashSet();
  protected WalPosition minimumRecoveryWalPosition = new WalPosition(0, 0);

  private int maxFileSize = 128 * 1024 * 1024; // 128m
  private int maxWalFileSize = 64 * 1024 * 1024;
  private int flushSize = 1000000;
  private int flushIntervalCount = 120;

  private transient OperatorContext context;

  static final byte[] DELETED = {};
  private transient HDHTWalManager wal;
  protected WalMeta singleWalMeta = new WalMeta();
  private long walKey = 0;
  private transient boolean bucketsRecovered = false;
  /**
   * Size limit for data files. Files are rolled once the limit has been exceeded. The final size of a file can be
   * larger than the limit by the size of the last/single entry written to it.
   *
   * @return The size limit for data files.
   */
  public int getMaxFileSize()
  {
    return maxFileSize;
  }

  public void setMaxFileSize(int maxFileSize)
  {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Size limit for WAL files. Files are rolled once the limit has been exceeded. The final size of a file can be larger
   * than the limit, as files are rolled at end of the operator window.
   *
   * @return The size limit for WAL files.
   */
  public int getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(int maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
    if (this.wal != null) {
      this.wal.setMaxWalFileSize(maxWalFileSize);
    }
  }

  /**
   * The number of changes collected in memory before flushing to persistent storage.
   *
   * @return The number of changes collected in memory before flushing to persistent storage.
   */
  public int getFlushSize()
  {
    return flushSize;
  }

  public void setFlushSize(int flushSize)
  {
    this.flushSize = flushSize;
  }

  /**
   * Cached writes are flushed to persistent storage periodically. The interval is specified as count of windows and
   * establishes the maximum latency for changes to be written while below the {@link #flushSize} threshold.
   *
   * @return The flush interval count.
   */
  @Min(value = 1)
  public int getFlushIntervalCount()
  {
    return flushIntervalCount;
  }

  public void setFlushIntervalCount(int flushIntervalCount)
  {
    this.flushIntervalCount = flushIntervalCount;
  }

  /**
   * Write data to size based rolling files
   *
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws IOException
   */
  private void writeFile(Bucket bucket, BucketMeta bucketMeta, TreeMap<Slice, Slice> data) throws IOException
  {
    BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    long startTime = System.currentTimeMillis();

    FileWriter fw = null;
    BucketFileMeta fileMeta = null;
    int keysWritten = 0;
    for (Map.Entry<Slice, Slice> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        LOG.debug("writing data file {} {}", bucket.bucketKey, fileMeta.name);
        fw = this.store.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
        keysWritten = 0;
      }

      if (Arrays.equals(dataEntry.getValue().toByteArray(), DELETED)) {
        continue;
      }

      fw.append(dataEntry.getKey().toByteArray(), dataEntry.getValue().toByteArray());
      keysWritten++;
      if (fw.getBytesWritten() > this.maxFileSize) {
        ioStats.dataFilesWritten++;
        ioStats.filesWroteInCurrentWriteCycle++;
        // roll file
        fw.close();
        ioStats.dataBytesWritten += fw.getBytesWritten();
        this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
        fw = null;
        keysWritten = 0;
      }
    }

    if (fw != null) {
      ioStats.dataFilesWritten++;
      ioStats.filesWroteInCurrentWriteCycle++;
      fw.close();
      ioStats.dataBytesWritten += fw.getBytesWritten();
      this.store.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
      LOG.debug("created data file {} {} with {} entries", bucket.bucketKey, fileMeta.name, keysWritten);
    }

    ioStats.dataWriteTime += System.currentTimeMillis() - startTime;
  }

  private Bucket getBucket(long bucketKey) throws IOException
  {
    Bucket bucket = this.buckets.get(bucketKey);
    bucketKeys.add(bucketKey);
    if (bucket == null) {
      LOG.debug("Opening bucket {}", bucketKey);
      bucket = new Bucket(keyComparator);
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);

      BucketMeta bmeta = getMeta(bucketKey);
      WalMeta wmeta = getWalMeta(bucketKey);
      LOG.info("walStart {} walEnd {} windowId {} committedWid {} currentWid {}",
          bmeta.recoveryStartWalPosition, wmeta.cpWalPosition, wmeta.windowId, bmeta.committedWid, currentWindowId);
      BucketIOStats ioStats = getOrCretaStats(bucketKey);
      if (ioStats != null) {
        this.wal.restoreStats(ioStats);
      }

      // bmeta.componentLSN is data which is committed to disks.
      // wmeta.windowId windowId till which data is available in WAL.
      if (!bucketsRecovered && bmeta.committedWid < wmeta.windowId && wmeta.windowId != 0) {
        LOG.debug("Recovery for buckets {}", bucketKeys);

        // Add tuples from recovery start till recovery end.

        Map<Long, WriteCache> bucketMap = Maps.newHashMap();

        for (Long bucketKeyEntry : bucketKeys) {
          Bucket bucketEntry = buckets.get(bucketKeyEntry);
          if (bucketEntry == null) {
            bucketEntry = new Bucket(keyComparator);
            bucketEntry.bucketKey = bucketKeyEntry;
            this.buckets.put(bucketKeyEntry, bucketEntry);
          }
          bucketMap.put(bucketKeyEntry, bucketEntry.committedWriteCache);
        }
        this.wal.runRecovery(new HDHTWalManager.RecoveryContext(bucketMap, keyComparator, minimumRecoveryWalPosition, wmeta.cpWalPosition));

        // After recovery data from WAL is added to committedCache, update location of WAL till data present in
        // committed cache.
        this.committedWalPosition = wmeta.cpWalPosition;
        this.walPositions.put(wmeta.windowId, wmeta.cpWalPosition);
        bucketsRecovered = true;
      }
    }
    return bucket;
  }

  /**
   * Lookup in write cache (data not flushed/committed to files).
   * @param bucketKey
   * @param key
   * @return uncommitted.
   */
  @Override
  public byte[] getUncommitted(long bucketKey, Slice key)
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket != null) {
      byte[] v = bucket.writeCache.get(key);
      if (v != null) {
        return v != DELETED ? v : null;
      }
      for (Map.Entry<Long, WriteCache> entry : bucket.checkpointedWriteCache.entrySet()) {
        byte[] v2 = entry.getValue().get(key);
        // find most recent entry
        if (v2 != null) {
          v = v2;
        }
      }
      if (v != null) {
        return v != DELETED ? v : null;
      }
      v = bucket.committedWriteCache.get(key);
      if (v != null) {
        return v != DELETED ? v : null;
      }
      v = bucket.frozenWriteCache.get(key);
      return v != null && v != DELETED ? v : null;
    }
    return null;
  }

  /**
   * Intercept query processing to incorporate unwritten changes.
   */
  @Override
  protected void processQuery(HDSQuery query)
  {
    // check unwritten changes first
    byte[] v = getUncommitted(query.bucketKey, query.key);
    if (v != null) {
      query.result = v;
      query.processed = true;
      return;
    }
    super.processQuery(query);
  }

  @Override
  public void put(long bucketKey, Slice key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    this.wal.append(bucketKey, key, value);
    bucket.writeCache.put(key, value);
    updateQueryResultCache(bucketKey, key, value);
  }

  private void updateQueryResultCache(long bucketKey, Slice key, byte[] value)
  {
    HDSQuery q = queries.get(key);
    if (q != null) {
      q.processed = true;
      q.result = value;
    }
  }

  public void delete(long bucketKey, Slice key) throws IOException
  {
    put(bucketKey, key, DELETED);
  }

  /**
   * Process purge operation performed.
   * Go over each file in the bucket, see if any purge operation affects the keys
   * present in the file. If any purge range overlaps with the file key range, then data needs
   * to be written again by removing keys in purge range.
   *
   * @param bucket bucket on which purge operations were performed.
   * @param bmeta  metadata for the bucket.
   * @param filesToDelete deleted files are added to this set.
   * @return new bucket meta copy after processing of purge operations.
   * @throws IOException
   */
  private BucketMeta processPurge(Bucket bucket, BucketMeta bmeta, HashSet<String> filesToDelete) throws IOException
  {
    /* Nothing to do if no files are written */
    if (bmeta.files.isEmpty()) {
      LOG.debug("No existing files to purge data from bucket {}", bucket);
      return bmeta;
    }

    /* no purge request pending */
    WriteCache frozen = bucket.frozenWriteCache;
    if (frozen.getPurges() == null || frozen.getPurges().isEmpty()) {
      LOG.debug("No Pending purge requests for bucket {}", bucket);
      return bmeta;
    }

    // make a copy, because as files are deleted in writeFileWithPurge, the traversal
    // of loop below will fail with concurrent modification exception.
    BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));

    Iterator<BucketFileMeta> fileIter = bmeta.files.values().iterator();
    Slice last = frozen.getPurges().getLast();

    while (fileIter.hasNext()) {
      BucketFileMeta fmeta = fileIter.next();
      /* If this file falls out of the last purge end value, then break
         as next files will be outside of purge range too.
       */
      if (keyComparator.compare(fmeta.startKey, last) > 0) {
        break;
      }
      Range<Slice> frange = new Range<>(fmeta.startKey, getEndKey(bucket.bucketKey, fmeta, frozen.getPurges()));
      RangeSet<Slice> rset = frozen.getPurges().getOverlappingRanges(frange);
      if (rset.isEmpty()) {
        continue;
      }

      writeFileWithPurge(bucket, fmeta, rset, filesToDelete, bucketMetaCopy);
    }
    return bucketMetaCopy;
  }

  /**
   * Write out the changes to the file because of purge operation. If any purge range completely
   * covers the keys in the file then delete the file without even opening and reading the keys
   * from the file.
   *
   * remove keys overlapping with purge ranges and they write out to a new file.
   * @param bucket bucket
   * @param meta file meta data.
   * @param rset purge range set which overlaps with the file.
   * @param filesToDelete if file is being deleted completely then add it to this list.
   * @param bmeta bucket metadata.
   * @throws IOException
   */
  private void writeFileWithPurge(Bucket bucket, BucketFileMeta meta, RangeSet<Slice> rset,
      HashSet<String> filesToDelete, BucketMeta bmeta) throws IOException
  {
    LOG.debug("Writing file because of purge operation {}", meta);

    if (rset.containsFully(new Range<>(meta.startKey, getEndKey(bucket.bucketKey, meta, rset)))) {
      LOG.info("File being deleted because of purge {}", meta);
      filesToDelete.add(meta.name);
      bmeta.files.remove(meta.startKey);
      return;
    }

    TreeMap<Slice, Slice> fileData = readDataExcludingPurge(bucket, meta, rset);

    /** Rewrite the file, if any key from file is removed as part of purge,
     * and there is some data to be written.
     */
    if (fileData.size() > 0) {
      LOG.info("Rewriting file because of purge {}", meta);
      filesToDelete.add(meta.name);
      bmeta.files.remove(meta.startKey);
      writeFile(bucket, bmeta, fileData);
    }
  }

  /**
   * Read all data from file, excluding the data which is masked by the purge ranges (rset).
   * This function use seek to seek at the end of current purge range to avoid reading keys
   * which are present in the purge range.
   *
   * @param bucket Bucket
   * @param meta metadata about file.
   * @param rset set of purge ranges.
   * @return data as a map.
   * @throws IOException
   */
  private TreeMap<Slice, Slice> readDataExcludingPurge(Bucket bucket, BucketFileMeta meta,
      RangeSet<Slice> rset) throws IOException
  {
    FileReader reader = store.getReader(bucket.bucketKey, meta.name);
    TreeMap<Slice, Slice> fileData = new TreeMap<>(keyComparator);

    /* Check if there is data in initial part of file before next purge range */
    Slice key = new Slice(null, 0, 0);
    Slice value = new Slice(null, 0, 0);

    boolean valid = reader.next(key, value);
    for (Range<Slice> range : rset) {
      while (keyComparator.compare(key, range.start) < 0 && valid) {
        fileData.put(new Slice(key.buffer, key.offset, key.length), new Slice(value.buffer));
        valid = reader.next(key, value);
      }
      /* need to check valid at every stage, because next wraps around the file
       * and starts reading from start of the file. */
      valid = reader.seek(range.end);
      if (!valid) {
        break;
      }
      valid = reader.next(key, value); // this will read end key, we want to exclude this key.
      if (!valid) {
        break;
      }
      valid = reader.next(key, value); // go past the end key.
      if (!valid) {
        break;
      }
    }
    while (valid) {
      fileData.put(new Slice(key.buffer, key.offset, key.length), new Slice(value.buffer));
      valid = reader.next(key, value);
    }
    return fileData;
  }

  /**
   * Flush changes from write cache to disk. New data files will be written and meta data replaced atomically. The flush
   * frequency determines availability of changes to external readers.
   *
   * @throws IOException
   */
  private void writeDataFiles(Bucket bucket) throws IOException
  {
    BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
    LOG.debug("Writing data files in bucket {}", bucket.bucketKey);
    // copy meta data on write
    BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));

    /** Process purge requests before flushing data from cache to maintain
     * the oder or purge and put operations. This makes sure that purged data
     * removed from file, before new data is added to the files */
    HashSet<String> filesToDelete = Sets.newHashSet();
    bucketMetaCopy = processPurge(bucket, bucketMetaCopy, filesToDelete);

    // bucket keys by file
    TreeMap<Slice, BucketFileMeta> bucketSeqStarts = bucketMetaCopy.files;
    Map<BucketFileMeta, Map<Slice, Slice>> modifiedFiles = Maps.newHashMap();

    for (Map.Entry<Slice, byte[]> entry : bucket.frozenWriteCache.entrySet()) {
      // find file for key
      Map.Entry<Slice, BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
      BucketFileMeta floorFile;
      if (floorEntry != null) {
        floorFile = floorEntry.getValue();
      } else {
        floorEntry = bucketSeqStarts.firstEntry();
        if (floorEntry == null || floorEntry.getValue().name != null) {
          // no existing file or file with higher key
          floorFile = new BucketFileMeta();
        } else {
          // placeholder for new keys, move start key
          floorFile = floorEntry.getValue();
          bucketSeqStarts.remove(floorEntry.getKey());
        }
        floorFile.startKey = entry.getKey();
        if (floorFile.startKey.length != floorFile.startKey.buffer.length) {
          // normalize key for serialization
          floorFile.startKey = new Slice(floorFile.startKey.toByteArray());
        }
        bucketSeqStarts.put(floorFile.startKey, floorFile);
      }

      Map<Slice, Slice> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey(), new Slice(entry.getValue()));
    }


    // write modified files
    for (Map.Entry<BucketFileMeta, Map<Slice, Slice>> fileEntry : modifiedFiles.entrySet()) {
      BucketFileMeta fileMeta = fileEntry.getKey();
      TreeMap<Slice, Slice> fileData = new TreeMap<Slice, Slice>(getKeyComparator());

      if (fileMeta.name != null) {
        // load existing file
        long start = System.currentTimeMillis();
        FileReader reader = store.getReader(bucket.bucketKey, fileMeta.name);
        reader.readFully(fileData);
        ioStats.dataBytesRead += store.getFileSize(bucket.bucketKey, fileMeta.name);
        ioStats.dataReadTime += System.currentTimeMillis() - start;
        /* these keys are re-written */
        ioStats.dataKeysRewritten += fileData.size();
        ioStats.filesReadInCurrentWriteCycle++;
        ioStats.dataFilesRead++;
        reader.close();
        filesToDelete.add(fileMeta.name);
      }

      // apply updates
      fileData.putAll(fileEntry.getValue());
      // new file
      writeFile(bucket, bucketMetaCopy, fileData);
    }

    LOG.debug("Files written {} files read {}", ioStats.filesWroteInCurrentWriteCycle,
        ioStats.filesReadInCurrentWriteCycle);
    // flush meta data for new files
    try {
      LOG.debug("Writing {} with {} file entries", FNAME_META, bucketMetaCopy.files.size());
      OutputStream os = store.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
      Output output = new Output(os);
      bucketMetaCopy.committedWid = bucket.committedLSN;
      bucketMetaCopy.recoveryStartWalPosition = bucket.recoveryStartWalPosition;
      kryo.writeClassAndObject(output, bucketMetaCopy);
      output.close();
      os.close();
      store.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
    }

    // clear pending changes
    ioStats.dataKeysWritten += bucket.frozenWriteCache.size();
    // switch to new version
    this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

    // delete old files
    for (String fileName : filesToDelete) {
      store.delete(bucket.bucketKey, fileName);
    }
    invalidateReader(bucket.bucketKey, filesToDelete);
    // clearing cache after invalidating readers
    bucket.frozenWriteCache.clear();

    // cleanup WAL files which are not needed anymore.
    minimumRecoveryWalPosition = bucketMetaCopy.recoveryStartWalPosition;
    for (Long bucketId : this.bucketKeys) {
      BucketMeta meta = getMeta(bucketId);
      if (meta.recoveryStartWalPosition.fileId < minimumRecoveryWalPosition.fileId || 
          (meta.recoveryStartWalPosition.fileId == minimumRecoveryWalPosition.fileId && 
          meta.recoveryStartWalPosition.offset < minimumRecoveryWalPosition.offset)) {
        minimumRecoveryWalPosition = meta.recoveryStartWalPosition;
      }
    }
    this.wal.cleanup(minimumRecoveryWalPosition.fileId);
    ioStats.filesReadInCurrentWriteCycle = 0;
    ioStats.filesWroteInCurrentWriteCycle = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (context != null) {
      setWalKey(context.getId());
    }
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName() + "-Writer"));

    this.context = context;
    if (this.walStore == null) {
      // if WAL location is not specified, by default it would be placed under <HDHT Location>/WAL/ if HDHT location is known
      // Otherwise default location is under <Application Path>/WAL/
      this.walStore = new TFileImpl.DTFileImpl();
      if (this.store instanceof FileAccessFSImpl) {
        ((FileAccessFSImpl)this.walStore).setBasePath(((FileAccessFSImpl)this.store).getBasePath() + WAL_FILES_LOCATION);
      } else {
        ((FileAccessFSImpl)this.walStore).setBasePath(context.getValue(DAG.APPLICATION_PATH) + WAL_FILES_LOCATION);
      }
    }
    this.walStore.init();

    if (!this.parentWals.isEmpty()) {
      if (this.parentWals.size() == 1) {
        PreviousWALDetails parentWal = parentWals.iterator().next();
        this.singleWalMeta = new WalMeta(parentWal.getWindowId(), parentWal.getEndPosition());
      } else {
        this.singleWalMeta.cpWalPosition = new WalPosition(0, 0);
      }
    }
    this.wal = new HDHTWalManager(this.walStore, getWalKey(), this.singleWalMeta.cpWalPosition);
    this.wal.setMaxWalFileSize(maxWalFileSize);

    if (!this.parentWals.isEmpty()) {
      resetBucketMeta();
      if (this.parentWals.size() == 1) {
        // Copy the WAL files as is from parent WAL
        this.walPositions = parentWals.iterator().next().walPositions;
        this.wal.copyPreviousWalFiles(parentWals, alreadyCopiedWals);
        alreadyCopiedWals.addAll(parentWals);
        parentWals.clear();
      } else {
        mergeParentWalFilesByWindow();
      }
    }
  }

  @Override
  public void teardown()
  {
    IOUtils.closeQuietly(this.wal);
    writeExecutor.shutdown();
    IOUtils.closeQuietly(this.walStore);
    super.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
      if (this.wal != null) {
        this.wal.endWindow(currentWindowId);
        singleWalMeta.cpWalPosition = this.wal.getCurrentPosition();
        singleWalMeta.windowId = currentWindowId;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to flush WAL", e);
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }

    if (context != null) {
      updateStats();
      context.setCounters(bucketStats);
    }
  }

  private WalMeta getWalMeta(long bucketKey)
  {
    return singleWalMeta;
  }

  @Override
  public void checkpointed(long windowId)
  {
    for (final Bucket bucket : this.buckets.values()) {
      if (!bucket.writeCache.isEmpty()) {
        bucket.checkpointedWriteCache.put(windowId, bucket.writeCache);
        bucket.writeCache = new WriteCache(new DefaultKeyComparator());
      }
    }
    this.walPositions.put(windowId, this.wal.getCurrentPosition());
  }

  /**
   * Get meta data from cache or load it on first access
   *
   * @param bucketKey
   * @return The bucket meta.
   */
  private BucketMeta getMeta(long bucketKey)
  {
    BucketMeta bm = metaCache.get(bucketKey);
    if (bm == null) {
      bm = loadBucketMeta(bucketKey);
      metaCache.put(bucketKey, bm);
    }
    return bm;
  }

  private void resetBucketMeta()
  {
    for (Long bucketKey : bucketKeys) {
      metaCache.put(bucketKey, new BucketMeta(keyComparator));
    }
  }

  @Override
  public void committed(long committedWindowId)
  {
    // Remove stale parent files if not already removed
    if (!alreadyCopiedWals.isEmpty()) {
      this.wal.deletePreviousWalFiles(alreadyCopiedWals);
      alreadyCopiedWals.clear();
    }
    for (final Bucket bucket : this.buckets.values()) {
      for (Iterator<Map.Entry<Long, WriteCache>> cpIter = bucket.checkpointedWriteCache.entrySet().iterator();
          cpIter.hasNext();) {
        Map.Entry<Long, WriteCache> checkpointEntry = cpIter.next();
        if (checkpointEntry.getKey() <= committedWindowId) {
          bucket.committedWriteCache.merge(checkpointEntry.getValue());
          cpIter.remove();
        }
      }

      for (Iterator<Map.Entry<Long, HDHTWalManager.WalPosition>> wpIter = this.walPositions.entrySet().iterator(); wpIter.hasNext();) {
        Map.Entry<Long, HDHTWalManager.WalPosition> entry = wpIter.next();
        if (entry.getKey() <= committedWindowId) {
          this.committedWalPosition = entry.getValue();
          wpIter.remove();
        }
      }

      if ((bucket.committedWriteCache.size() > this.flushSize || currentWindowId - bucket.lastFlushWindowId > flushIntervalCount) && !bucket.committedWriteCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.committedWriteCache;
          bucket.committedWriteCache = new WriteCache(keyComparator);
          bucket.recoveryStartWalPosition = this.committedWalPosition;
          bucket.committedLSN = committedWindowId;

          LOG.debug("Flushing data for bucket {} committedWid {} recoveryStartWalPosition {}",
              bucket.bucketKey, bucket.committedLSN, bucket.recoveryStartWalPosition);
          Runnable flushRunnable = new Runnable()
          {
            @Override
            public void run()
            {
              try {
                writeDataFiles(bucket);
              } catch (Throwable e) {
                LOG.debug("Write error: {}", e.getMessage());
                writerError = e;
              }
            }
          };
          this.writeExecutor.execute(flushRunnable);
          bucket.lastFlushWindowId = committedWindowId;
        }
      }
    }

    // propagate writer exceptions
    if (writerError != null) {
      throw new RuntimeException("Error while flushing write cache.", this.writerError);
    }
  }

  private static class Bucket
  {
    private long lastFlushWindowId;
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted, by checkpoint
    private WriteCache writeCache = new WriteCache(new DefaultKeyComparator());
    private final LinkedHashMap<Long, WriteCache> checkpointedWriteCache = Maps.newLinkedHashMap();
    private WriteCache committedWriteCache = new WriteCache(new DefaultKeyComparator());
    // keys that are being flushed to data files
    private WriteCache frozenWriteCache = new WriteCache(new DefaultKeyComparator());
    private long committedLSN;
    public HDHTWalManager.WalPosition recoveryStartWalPosition;

    public Bucket(Comparator<Slice> cmp)
    {
      writeCache = new WriteCache(cmp);
      committedWriteCache = new WriteCache(cmp);
      frozenWriteCache = new WriteCache(cmp);
    }
  }

  @VisibleForTesting
  protected void forceWal() throws IOException
  {
    this.wal.close();
  }

  @VisibleForTesting
  protected int unflushedDataSize(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.writeCache.size();
  }

  @VisibleForTesting
  protected int committedDataSize(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.committedWriteCache.size();
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDHTWriter.class);

  /* Holds current file Id for WAL and current recoveryEndWalOffset for WAL */
  static class WalMeta
  {
    /* The current WAL file and recoveryEndWalOffset */
    // Window Id which is written to the WAL.
    public long windowId;

    // Checkpointed WAL position.
    HDHTWalManager.WalPosition cpWalPosition;

    public WalMeta(long windowId, WalPosition walPosition)
    {
      this.windowId = windowId;
      this.cpWalPosition = walPosition;
    }

    public WalMeta()
    {
    }
  }

  @JsonSerialize
  public static class BucketIOStats implements Serializable
  {
    private static final long serialVersionUID = 201412091454L;
    /* Bytes written to the WAL till now */
    public long walBytesWritten;
    /* Number of times WAL was flushed */
    public long walFlushCount;
    /* Amount of time spent while waiting for WAL flush to disk in milliseconds */
    public long walFlushTime;
    /* wal keys written */
    public long walKeysWritten;


    /* Number of data files written */
    public long dataFilesWritten;
    /* Number of bytes written to data files */
    public long dataBytesWritten;
    /* Time taken for writing files */
    public long dataWriteTime;
    /* total keys written to data files */
    public long dataKeysWritten;
    /* The number of keys which are re-written, i.e keys which are read into memory from existing
       files which are written again in new data files */
    public long dataKeysRewritten;

    /* records in memory */
    public long dataInWriteCache;
    public long dataInFrozenCache;
    public int filesReadInCurrentWriteCycle;
    public int filesWroteInCurrentWriteCycle;

    public int dataFilesRead;
    /* Total time spent in reading files during write */
    public long dataReadTime;
    /* Number of bytes read during data read */
    public long dataBytesRead;

    @Override
    public String toString()
    {
      return "BucketIOStats{" +
          "walBytesWritten=" + walBytesWritten +
          ", walFlushCount=" + walFlushCount +
          ", walFlushTime=" + walFlushTime +
          ", walKeysWritten=" + walKeysWritten +
          ", dataFilesWritten=" + dataFilesWritten +
          ", dataBytesWritten=" + dataBytesWritten +
          ", dataWriteTime=" + dataWriteTime +
          ", dataKeysWritten=" + dataKeysWritten +
          ", dataKeysRewritten=" + dataKeysRewritten +
          ", dataInWriteCache=" + dataInWriteCache +
          ", dataInFrozenCache=" + dataInFrozenCache +
          ", filesReadInCurrentWriteCycle=" + filesReadInCurrentWriteCycle +
          ", filesWroteInCurrentWriteCycle=" + filesWroteInCurrentWriteCycle +
          ", dataFilesRead=" + dataFilesRead +
          ", dataReadTime=" + dataReadTime +
          ", dataBytesRead=" + dataBytesRead +
          '}';
    }
  }

  private void updateStats()
  {
    for (Bucket bucket : buckets.values()) {
      BucketIOStats ioStats = getOrCretaStats(bucket.bucketKey);
      /* fill in stats for WAL */
      HDHTWalManager.WalStats walStats = this.wal.getCounters();
      ioStats.walBytesWritten = walStats.totalBytes;
      ioStats.walFlushCount = walStats.flushCounts;
      ioStats.walFlushTime = walStats.flushDuration;
      ioStats.walKeysWritten = walStats.totalKeys;
      ioStats.dataInWriteCache = bucket.writeCache.size();
      ioStats.dataInFrozenCache = bucket.frozenWriteCache.size();
    }
  }

  @JsonSerialize
  public static class AggregatedBucketIOStats implements Serializable
  {
    private static final long serialVersionUID = 201412091454L;
    public BucketIOStats globalStats = new BucketIOStats();
    /* Individual bucket stats */
    public Map<Long, BucketIOStats> aggregatedStats = Maps.newHashMap();
  }

  public static class BucketIOStatAggregator implements Serializable, Context.CountersAggregator
  {
    private static final long serialVersionUID = 201412091454L;

    @Override
    public Object aggregate(Collection<?> countersList)
    {
      AggregatedBucketIOStats aggStats = new AggregatedBucketIOStats();
      for (Object o : countersList) {
        @SuppressWarnings("unchecked")
        Map<Long, BucketIOStats> statMap = (Map<Long, BucketIOStats>)o;
        for (Long bId : statMap.keySet()) {
          BucketIOStats stats = statMap.get(bId);
          aggStats.globalStats.walBytesWritten += stats.walBytesWritten;
          aggStats.globalStats.walFlushCount += stats.walFlushCount;
          aggStats.globalStats.walFlushTime += stats.walFlushTime;
          aggStats.globalStats.walKeysWritten += stats.walKeysWritten;

          aggStats.globalStats.dataWriteTime += stats.dataWriteTime;
          aggStats.globalStats.dataFilesWritten += stats.dataFilesWritten;
          aggStats.globalStats.dataBytesWritten += stats.dataBytesWritten;
          aggStats.globalStats.dataKeysWritten += stats.dataKeysWritten;
          aggStats.globalStats.dataKeysRewritten += stats.dataKeysRewritten;

          aggStats.globalStats.dataInWriteCache += stats.dataInWriteCache;
          aggStats.globalStats.dataInFrozenCache += stats.dataInFrozenCache;
          aggStats.globalStats.filesReadInCurrentWriteCycle += stats.filesReadInCurrentWriteCycle;
          aggStats.globalStats.filesWroteInCurrentWriteCycle += stats.filesWroteInCurrentWriteCycle;

          aggStats.globalStats.dataReadTime += stats.dataReadTime;
          aggStats.globalStats.dataFilesRead += stats.dataFilesRead;
          aggStats.globalStats.dataBytesRead += stats.dataBytesRead;

          aggStats.aggregatedStats.put(bId, stats);
        }
      }
      return aggStats;
    }
  }

  /* A map holding stats for each bucket written by this partition */
  private final HashMap<Long, BucketIOStats> bucketStats = Maps.newHashMap();

  private BucketIOStats getOrCretaStats(long bucketKey)
  {
    BucketIOStats ioStats = bucketStats.get(bucketKey);
    if (ioStats == null) {
      ioStats = new BucketIOStats();
      bucketStats.put(bucketKey, ioStats);
    }
    return ioStats;
  }

  @Override
  public void purge(long bucketKey, Slice start, Slice end) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    this.wal.append(new HDHTLogEntry.PurgeEntry(bucketKey, start, end));
    bucket.writeCache.purge(start, end);
  }


  /**
   * Returns approximate end key of the file.
   *
   * To avoid reading whole file this function returns start key of the next file.
   * This information is available from metadata and does not require any disk I/O.
   *
   * For the last file in the list, If any purge ranges are provided, then go over each range
   * in range set and check if we could seek to endKey successfully,
   * successful seek means that the file contain key greater than range's endKey.
   * In this case try the next range. This approach works as range set is sorted by the start and end keys,
   * and it does not contain any overlapping ranges (overlapping ranges are already taken care during
   * merge of the ranges).
   *
   * If file contains data beyond last range provided, then we have no option other than
   * to read the file and return the last key. We can not generate the next key from last
   * range's endKey and we do not have knowledge about the comparator
   *
   */
  protected Slice getEndKey(long bucketKey, BucketFileMeta fmeta, RangeSet<Slice> rset) throws IOException
  {
    BucketMeta bmeta = getMeta(bucketKey);
    Map.Entry<Slice, BucketFileMeta> entry = bmeta.files.higherEntry(fmeta.startKey);
    if (entry != null) {
      return entry.getKey();
    }
    boolean valid = true;
    FileReader reader = store.getReader(bucketKey, fmeta.name);
    if (rset != null) {
      for (Range<Slice> range : rset) {
        valid = reader.seek(range.end);
        if (!valid) {
          return range.end;
        }
      }
    }
    Slice key = new Slice(null, 0, 0);
    Slice value = new Slice(null, 0, 0);
    while (valid) {
      valid = reader.next(key, value);
    }
    return key;
  }

  public FileAccess getWalStore()
  {
    return walStore;
  }

  public void setWalStore(FileAccess walStore)
  {
    this.walStore = walStore;
  }

  /**
   * Merge multiple parent WAL files into single WAL file by copying parent WAL entries ordered by Window Ids
   */
  public void mergeParentWalFilesByWindow()
  {
    try {
      // Copy all the WAL file contents which are committed and removed from checkpointed window Position states
      // Order of window ID need not be maintained for already committed WAL. So copy by appending.
      for (PreviousWALDetails parentWal : parentWals) {
        if (parentWal.getCommittedWalPosition() != null) {
          this.wal.copyWALFiles(parentWal.getStartPosition(), parentWal.getCommittedWalPosition(), parentWal.getWalKey());
        }
      }
      for (Long bucketKey : bucketKeys) {
        BucketMeta meta = metaCache.get(bucketKey);
        meta.recoveryStartWalPosition = this.wal.getCurrentPosition();
      }
      // Copy remaining checkpointed window positions ordered by windowId
      this.wal.mergeWalFiles(parentWals, walPositions);
      this.forceWal();
      this.wal.writer = null;
      alreadyCopiedWals.addAll(parentWals);
      parentWals.clear();
      singleWalMeta.cpWalPosition = this.wal.getCurrentPosition();
      // Reset WAL recovery position to beginning of WAL files
      minimumRecoveryWalPosition = new WalPosition(0, 0);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public long getWalKey()
  {
    return walKey;
  }

  public void setWalKey(long walKey)
  {
    this.walKey = walKey;
  }
}
