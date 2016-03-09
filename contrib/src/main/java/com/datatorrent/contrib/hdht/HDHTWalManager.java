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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import com.datatorrent.contrib.hdht.wal.FSWALReader;
import com.datatorrent.contrib.hdht.wal.FSWALWriter;
import com.datatorrent.contrib.hdht.wal.WALReader;
import com.datatorrent.contrib.hdht.wal.WALWriter;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.netlet.util.Slice;

/**
 * Manages WAL for multiple buckets. When a tuple is added to WAL, is it
 * immediately written to the file, but not flushed, flushing happens at end of
 * the operator window during endWindow call. At end of window if WAL file size
 * have grown a beyond maxWalFileSize then current file is closed and new file
 * is created.
 *
 * The WAL usage windowId as log sequence number(LSN). When data is
 * written to data files, the committedWid saved in bucket metadata.
 *
 * The windowId upto which data is available is stored in BucketManager
 * WalMetadata and checkpointed with operator state.
 *
 * Recovery After Failure.
 *
 *   If committedWid is smaller than wal windowId.
 *   - Truncate last WAL file to known offset (recoveryEndWalOffset).
 *   - Wal metadata contains file id and recoveryEndWalOffset where committedWid ended,
 *     start reading from that location till the end of current WAL file
 *     and adds tuples back to the committed cache in store.
 *
 *   If committedWid is greater than wal windowId
 *   The data was committed to disks after last operator checkpoint. In this
 *   case recovery is not needed all data from WAL is already written to data
 *   files. We will reprocess tuples which are in between committedWid and wal windowId.
 *   This will not cause problem now, because file write is idempotent with
 *   duplicate tuples.
 *
 * @since 2.0.0
 */
public class HDHTWalManager implements Closeable
{
  public static final String WAL_FILE_PREFIX = "_WAL-";
  private transient byte[] copyBuffer;

  public void setWalKey(long bucketKey)
  {
    this.walKey = bucketKey;
  }

  public void restoreStats(HDHTWriter.BucketIOStats ioStats)
  {
    if (stats != null) {
      stats.flushCounts = ioStats.walFlushCount;
      stats.flushDuration = ioStats.walFlushTime;
      stats.totalBytes = ioStats.walBytesWritten;
      stats.totalKeys = ioStats.walKeysWritten;
    }
  }

  /* Backing file system for WAL */
  transient FileAccess bfs;

  /* Maximum number of bytes per WAL file,
   * default is 128M */
  transient long maxWalFileSize = 128 * 1024 * 1024;

  /* The class responsible writing WAL entry to file */
  transient WALWriter<HDHTLogEntry.HDHTWalEntry> writer;

  private transient long walKey;

  private boolean dirty;

  /* Last committed LSN on disk */
  private long flushedWid = -1;

  /* current active WAL file id, it is read from WAL meta on startup */
  private long walFileId = -1;

  /* Current WAL size */
  private long walSize = 0;

  @SuppressWarnings("unused")
  private HDHTWalManager()
  {
  }

  public HDHTWalManager(FileAccess bfs, long walKey)
  {
    this.bfs = bfs;
    this.walKey = walKey;
  }

  public HDHTWalManager(FileAccess bfs, long walKey, WalPosition walPos)
  {
    this(bfs, walKey);
    this.walFileId = walPos == null ? 0 : walPos.fileId;
    this.walSize = walPos == null ? 0 : walPos.offset;
    logger.info("current {}  offset {} ", walFileId, walSize);
  }

  public HDHTWalManager(FileAccess bfs, long walKey, long fileId, long offset)
  {
    this.bfs = bfs;
    this.walKey = walKey;
    this.walFileId = fileId;
    this.walSize = offset;
    logger.info("current {}  offset {} ", walFileId, walSize);
  }

  @Deprecated
  public void runRecovery(Map<Slice, byte[]> writeCache, WalPosition start, WalPosition end) throws IOException
  {
    // not used just for backward compatibility.
  }

  /**
   * Run recovery for bucket, by adding valid data from WAL to store.
   */
  public void runRecovery(RecoveryContext context) throws IOException
  {
    if (context.endWalPos.fileId == 0 && context.endWalPos.offset == 0) {
      return;
    }

    /* Make sure that WAL state is correctly restored */
    truncateWal(context.endWalPos);

    logger.info("Recovery of store, start {} till {}", context.startWalPos, context.endWalPos);

    long offset = context.startWalPos.offset;
    for (long i = context.startWalPos.fileId; i <= context.endWalPos.fileId; i++) {
      WALReader<HDHTLogEntry.HDHTWalEntry> wReader = new FSWALReader<HDHTLogEntry.HDHTWalEntry>(bfs, new HDHTLogEntry.HDHTLogSerializer(), walKey, WAL_FILE_PREFIX + i);
      wReader.seek(offset);
      offset = 0;
      int count = 0;
      while (wReader.advance()) {
        HDHTLogEntry.HDHTWalEntry savedEntry = wReader.get();
        // TODO: Handle unnecessary recovery for buckets
        recoveryEntry(context, savedEntry);
        count++;
      }
      wReader.close();
      logger.info("Recovered {} tuples from wal {}", count, i);
    }

    walFileId++;
  }

  private void recoveryEntry(RecoveryContext context, HDHTLogEntry.HDHTWalEntry entry)
  {
    WriteCache writeCache = context.bucketKeysWriteCacheMap.get(entry.getBucket());
    if (writeCache == null) {
      // Skip recovery if bucket is not managed by partition
      return;
    }
    if (entry instanceof HDHTLogEntry.PutEntry) {
      HDHTLogEntry.PutEntry putEntry = (HDHTLogEntry.PutEntry)entry;
      writeCache.put(putEntry.key, putEntry.val);
    } else if (entry instanceof HDHTLogEntry.DeleteEntry) {
      writeCache.put(((HDHTLogEntry.DeleteEntry)entry).key, HDHTWriter.DELETED);
    } else if (entry instanceof HDHTLogEntry.PurgeEntry) {
      HDHTLogEntry.PurgeEntry pEntry = (HDHTLogEntry.PurgeEntry)entry;
      writeCache.purge(pEntry.startKey, pEntry.endKey);
      logger.debug("processing purge command {}", entry);
    }
  }

  /**
   * Restore state of wal just after last checkpoint. The Apex platform will
   * resend tuple after last operator checkpoint to the WAL, this will result in
   * duplicate tuples in WAL, if we don't restore the WAL just after checkpoint
   * state.
   */
  private void truncateWal(WalPosition pos) throws IOException
  {
    if (pos.offset == 0) {
      return;
    }
    logger.info("recover wal file {}, data valid till offset {}", pos.fileId, pos.offset);
    DataInputStream in = bfs.getInputStream(walKey, WAL_FILE_PREFIX + pos.fileId);
    DataOutputStream out = bfs.getOutputStream(walKey, WAL_FILE_PREFIX + pos.fileId + "-truncate");
    IOUtils.copyLarge(in, out, 0, pos.offset);
    in.close();
    out.close();
    bfs.rename(walKey, WAL_FILE_PREFIX + pos.fileId + "-truncate", WAL_FILE_PREFIX + pos.fileId);
  }

  public void append(long buckeyKey, Slice key, byte[] value) throws IOException
  {
    append(new HDHTLogEntry.PutEntry(buckeyKey, key, value));
    stats.totalKeys++;
  }

  public void append(HDHTLogEntry.HDHTWalEntry entry) throws IOException
  {

    if (writer == null) {
      writer = new FSWALWriter<HDHTLogEntry.HDHTWalEntry>(bfs, new HDHTLogEntry.HDHTLogSerializer(), walKey, WAL_FILE_PREFIX + walFileId);
    }

    int len = writer.append(entry);
    stats.totalBytes += len;
    dirty = true;
  }

  public void append(byte[] buffer, int length) throws IOException
  {

    if (writer == null) {
      writer = new FSWALWriter<HDHTLogEntry.HDHTWalEntry>(bfs, new HDHTLogEntry.HDHTLogSerializer(), walKey, WAL_FILE_PREFIX + walFileId);
    }

    writer.append(buffer, length);
    stats.totalBytes += length;
  }

  protected void flushWal() throws IOException
  {
    if (writer == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    writer.flush();

    stats.flushCounts++;
    stats.flushDuration += System.currentTimeMillis() - startTime;
  }

  /* batch writes, and wait till file is written */
  public void endWindow(long windowId) throws IOException
  {
    /* No tuple added in this window, no need to do anything. */
    if (!dirty) {
      return;
    }

    flushWal();

    dirty = false;
    flushedWid = windowId;
    walSize = writer.getSize();

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.getSize() > maxWalFileSize) {
      logger.info("Rolling over log {} windowid {}", writer, windowId);
      writer.close();
      walFileId++;
      writer = null;
      walSize = 0;
    }
  }

  /**
   * Remove files older than recoveryStartWalFileId.
   * 
   * @param recoveryStartWalFileId
   */
  public void cleanup(long recoveryStartWalFileId)
  {
    if (recoveryStartWalFileId == 0) {
      return;
    }

    recoveryStartWalFileId--;
    try {
      while (true) {
        DataInputStream in = bfs.getInputStream(walKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        in.close();
        logger.info("deleting WAL file {}", recoveryStartWalFileId);
        bfs.delete(walKey, WAL_FILE_PREFIX + recoveryStartWalFileId);
        recoveryStartWalFileId--;
      }
    } catch (FileNotFoundException ex) {
      //Do nothing
    } catch (IOException ex) {
      //Do nothing
    }
  }

  public long getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(long maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  @Deprecated
  public long getMaxUnflushedBytes()
  {
    return Long.MAX_VALUE;
  }

  @Deprecated
  public void setMaxUnflushedBytes(long maxUnflushedBytes)
  {
  }

  public long getFlushedWid()
  {
    return flushedWid;
  }

  @Override
  public void close() throws IOException
  {
    if (writer != null) {
      writer.close();
    }
  }

  public long getWalFileId()
  {
    return walFileId;
  }

  public long getWalSize()
  {
    return walSize;
  }

  public void setFileStore(FileAccess bfs)
  {
    this.bfs = bfs;
  }

  public WalPosition getCurrentPosition()
  {
    return new WalPosition(walFileId, walSize);
  }

  private static final transient Logger logger = LoggerFactory.getLogger(HDHTWalManager.class);

  static class RecoveryContext
  {
    WalPosition startWalPos;
    WalPosition endWalPos;
    Map<Long, WriteCache> bucketKeysWriteCacheMap;

    public RecoveryContext(Map<Long, WriteCache> writeCacheMap, Comparator<Slice> cmparator, WalPosition startWalPos, WalPosition endWalPos)
    {
      this.bucketKeysWriteCacheMap = writeCacheMap;
      this.startWalPos = startWalPos;
      this.endWalPos = endWalPos;
    }
  }

  /**
   * Stats related functionality
   */
  public static class WalStats
  {
    long totalBytes;
    long flushCounts;
    long flushDuration;
    public long totalKeys;
  }

  private final WalStats stats = new WalStats();
  private int BUFFER_SIZE = 65536;

  /* Location of the WAL */
  public static class WalPosition
  {
    protected long fileId;
    protected long offset;

    public WalPosition()
    {
    }

    public WalPosition(long fileId, long offset)
    {
      this.fileId = fileId;
      this.offset = offset;
    }

    public WalPosition copyOf()
    {
      return new WalPosition(fileId, offset);
    }

    @Override
    public String toString()
    {
      return "WalPosition{" + "fileId=" + fileId + ", offset=" + offset + '}';
    }
  }

  public WalStats getCounters()
  {
    return stats;
  }

  public void copyPreviousWalFiles(List<PreviousWALDetails> parentWals, Set<PreviousWALDetails> alreadyCopiedWals)
  {
    try {
      PreviousWALDetails parentWal = parentWals.iterator().next();
      // Copy Files to new WAL location
      for (long i = parentWal.getStartPosition().fileId; i <= parentWal.getEndPosition().fileId; i++) {
        DataInputStream in = bfs.getInputStream(parentWal.getWalKey(), WAL_FILE_PREFIX + i);
        DataOutputStream out = bfs.getOutputStream(walKey, WAL_FILE_PREFIX + i);
        IOUtils.copyLarge(in, out);
        in.close();
        out.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void copyWalPart(WalPosition startPosition, WalPosition endPosition, long oldWalKey)
  {
    try {
      if (bfs.exists(oldWalKey, WAL_FILE_PREFIX + endPosition.fileId)) {
        DataInputStream in = bfs.getInputStream(oldWalKey, WAL_FILE_PREFIX + endPosition.fileId);
        int length = (int)(startPosition.fileId == endPosition.fileId ? endPosition.offset - startPosition.offset : endPosition.offset);
        int offset = (int)(startPosition.fileId == endPosition.fileId ? startPosition.offset : 0);
        logger.info("length = {} offset = {} start offset = {} end offset = {} File = {}", length, offset, startPosition, endPosition);
        if (copyBuffer == null) {
          copyBuffer = new byte[BUFFER_SIZE];
        }
        IOUtils.skip(in, offset);
        while (length > 0) {
          int readBytes = IOUtils.read(in, copyBuffer, 0, length < BUFFER_SIZE ? length : BUFFER_SIZE);
          append(copyBuffer, readBytes);
          length -= readBytes;
        }
        in.close();

        flushWal();
        if (writer != null) {
          walSize = writer.getSize();
        }
        logger.debug("wal size so far = {}", walSize);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Copy old WAL files to current location from startPosition to End Position in old WAL.
   * @param startPosition
   * @param endPosition
   * @param oldWalKey
   */
  public void copyWALFiles(WalPosition startPosition, WalPosition endPosition, long oldWalKey)
  {
    try {
      for (long i = startPosition.fileId; i < endPosition.fileId; i++) {
        if (bfs.exists(oldWalKey, WAL_FILE_PREFIX + i)) {
          DataInputStream in = bfs.getInputStream(oldWalKey, WAL_FILE_PREFIX + i);
          DataOutputStream out = bfs.getOutputStream(walKey, WAL_FILE_PREFIX + walFileId);

          IOUtils.copyLarge(in, out);
          in.close();
          out.close();
          walFileId++;
        }
      }
      // Copy last file upto end position offset
      copyWalPart(startPosition, endPosition, oldWalKey);
      if (maxWalFileSize > 0 && walSize > maxWalFileSize) {
        writer.close();
        writer = null;
        walFileId++;
        walSize = 0;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Delete old parent WAL files which are already copied to new WAL location
   * @param alreadyCopiedWals
   */
  public void deletePreviousWalFiles(Set<PreviousWALDetails> alreadyCopiedWals)
  {
    try {
      for (Iterator<PreviousWALDetails> it = alreadyCopiedWals.iterator(); it.hasNext();) {
        PreviousWALDetails parentWal = it.next();
        // TODO: If using file APIs, delete entire folder for old WAL files
        logger.debug("Deleting WAL file {}", parentWal.getWalKey());
        // delete WAL Files if not already deleted
        for (long i = parentWal.getStartPosition().fileId; i <= parentWal.getEndPosition().fileId; i++) {
          if (bfs.exists(parentWal.getWalKey(), WAL_FILE_PREFIX + i)) {
            bfs.delete(parentWal.getWalKey(), WAL_FILE_PREFIX + i);
          }
        }
        alreadyCopiedWals.remove(it);
      }
    } catch (IOException e) {
      logger.warn("Exception occurred while deleting old WAL files {}:{}", e.getMessage(), e.getStackTrace());
    }
  }

  private static class WalWindowPosition
  {
    public Long walKey;
    public Long windowId;
    public WalPosition walPosition;

    public WalWindowPosition(Long walKey, Long windowId, WalPosition walPosition)
    {
      this.walKey = walKey;
      this.windowId = windowId;
      this.walPosition = walPosition;
    }
  }

  /**
   * Copy content from parent WAL files to new location ordered by WindowID.
   * @param parentWals
   * @param walPositions
   */
  public void mergeWalFiles(List<PreviousWALDetails> parentWals, HashMap<Long, WalPosition> walPositions)
  {
    Map<Long, Iterator<Map.Entry<Long, WalPosition>>> iteratorsMap = Maps.newHashMap();
    Map<Long, WalPosition> startPositionMap = Maps.newHashMap();

    for (PreviousWALDetails walDetails : parentWals) {
      Iterator<Map.Entry<Long, WalPosition>> it = walDetails.walPositions.entrySet().iterator();
      iteratorsMap.put(walDetails.getWalKey(), it);
      if (walDetails.getCommittedWalPosition() != null) {
        startPositionMap.put(walDetails.getWalKey(), walDetails.getCommittedWalPosition());
      } else {
        startPositionMap.put(walDetails.getWalKey(), new WalPosition(0, 0));
      }
    }

    PriorityQueue<WalWindowPosition> currentValues = new PriorityQueue<>(parentWals.size(), new Comparator<WalWindowPosition>()
    {
      @Override
      public int compare(WalWindowPosition o1, WalWindowPosition o2)
      {
        return (int)(o1.windowId - o2.windowId);
      }
    });

    do {
      for (Map.Entry<Long, Iterator<Map.Entry<Long, WalPosition>>> entry : iteratorsMap.entrySet()) {
        if (entry.getValue().hasNext()) {
          Map.Entry<Long, WalPosition> windowWalPosition = entry.getValue().next();
          currentValues.add(new WalWindowPosition(entry.getKey(), windowWalPosition.getKey(), windowWalPosition.getValue()));
        }
      }
      if (!currentValues.isEmpty()) {
        WalWindowPosition minWindowWalEntry = currentValues.remove();
        copyWALFiles(startPositionMap.get(minWindowWalEntry.walKey), minWindowWalEntry.walPosition, minWindowWalEntry.walKey);
        // Set next start position for WAL key
        startPositionMap.put(minWindowWalEntry.walKey, minWindowWalEntry.walPosition);
        // Set end position for windowId for checkpointed positions
        walPositions.put(minWindowWalEntry.windowId, this.getCurrentPosition());
      }
    } while (!currentValues.isEmpty());
  }

  /**
   * Contains details relevant for restoring state of WAL files after dynamic repartitioning
   *
   */
  public static class PreviousWALDetails implements Serializable
  {
    @Override
    public String toString()
    {
      return "PreviousWALDetails [startPosition=" + startPosition + ", endPosition=" + endPosition +
          ", committedWalPosition=" + committedWalPosition + ", walKey=" + walKey + ", windowId=" +
          windowId + ", walPositions=" + walPositions + "]";
    }

    private static final long serialVersionUID = 4648909072979382021L;
    private WalPosition startPosition;
    private WalPosition endPosition;
    private long windowId;
    private WalPosition committedWalPosition;
    private long walKey;
    public HashMap<Long, HDHTWalManager.WalPosition> walPositions = Maps.newLinkedHashMap();

    public PreviousWALDetails()
    {
    }

    public PreviousWALDetails(long walKey, WalPosition startPosition, WalPosition endPosition, HashMap<Long, WalPosition> walPositions, WalPosition committedWalPosition, long windowId)
    {
      this.walKey = walKey;
      this.startPosition = startPosition;
      this.endPosition = endPosition;
      this.walPositions = walPositions;
      this.setCommittedWalPosition(committedWalPosition);
      this.setWindowId(windowId);
    }

    public WalPosition getStartPosition()
    {
      return startPosition;
    }

    public void setStartPosition(WalPosition startPosition)
    {
      this.startPosition = startPosition;
    }

    public WalPosition getEndPosition()
    {
      return endPosition;
    }

    public void setEndPosition(WalPosition endPosition)
    {
      this.endPosition = endPosition;
    }

    public long getWalKey()
    {
      return walKey;
    }

    public void setWalKey(long walKey)
    {
      this.walKey = walKey;
    }

    public boolean needsRecovery()
    {
      if (this.endPosition == null || (this.startPosition.fileId == this.endPosition.fileId && this.startPosition.offset == this.endPosition.offset)) {
        return false;
      }
      return true;
    }

    public WalPosition getCommittedWalPosition()
    {
      return committedWalPosition;
    }

    public void setCommittedWalPosition(WalPosition committedWalPosition)
    {
      this.committedWalPosition = committedWalPosition;
    }

    public long getWindowId()
    {
      return windowId;
    }

    public void setWindowId(long windowId)
    {
      this.windowId = windowId;
    }
  }
}
