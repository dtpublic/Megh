/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Operator.CheckpointListener;

/**
 * An implementation of an Expirable Bucket Store which can delete files asynchronously and safely. Every time a file is
 * required to be deleted, it is noted down in {@link #deletedFiles} along with the window in which it was deleted.
 * Additionally the caller also needs to call {@link #captureFilesToDelete(long)} in order to record the files that are
 * deleted in the window id passed as a parameter. At the committed call, all the files in the given commit window are
 * selected and deleted.
 *
 * @param <T>
 */
public class ExpirableHdfsBucketStoreAsync<T> extends ExpirableHdfsBucketStore<T> implements CheckpointListener
{
  protected Set<String> deletedFiles;
  protected Map<Long, Set<String>> deleteSnapshot;

  public ExpirableHdfsBucketStoreAsync()
  {
    super();
    deletedFiles = Sets.newHashSet();
    deleteSnapshot = Maps.newConcurrentMap();
  }

  /**
   * This method marks the file as deleted in the data structures. Additionally only records the files to be deleted
   * from the persistent store. Does not actually delete them.
   */
  @Override
  public void deleteExpiredBuckets(long time)
  {
    Iterator<Long> iterator = windowToBuckets.keySet().iterator();
    for (; iterator.hasNext();) {
      long window = iterator.next();
      long timestamp = windowToTimestamp.get(window);
      if (timestamp < time) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          if (indices.size() > 0) {
            deletedFiles.add(Long.toString(window));
            for (int bucketIdx : indices) {
              Map<Long, Long> offsetMap = bucketPositions[bucketIdx];
              if (offsetMap != null) {
                synchronized (offsetMap) {
                  offsetMap.remove(window);
                }
              }
            }
          }
          windowToTimestamp.remove(window);
          iterator.remove();
        }
      }
    }
  }

  /**
   * Records the file to be deleted in {@link #deletedFiles}
   */
  @Override
  protected void deleteFile(String fileName) throws IOException
  {
    deletedFiles.add(fileName);
  }

  /**
   * Deletes all files listed in {@link #deleteSnapshot} for parameter windowId. Also cleans up the
   * {@link #deleteSnapshot} for window ids < parameter windowId
   *
   * @param windowId
   * @throws IOException
   */
  protected void processPendingDeletes(long windowId) throws IOException
  {
    if (deleteSnapshot.isEmpty() || !(deleteSnapshot.containsKey(windowId))) {
      return;
    }
    Iterator<String> files = deleteSnapshot.get(windowId).iterator();
    FileSystem fs = FileSystem.newInstance(configuration);
    try {
      while (files.hasNext()) {
        String fileName = files.next();
        Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + fileName);
        if (fs.exists(dataFilePath)) {
          logger.debug("start delete {}", fileName);
          fs.delete(dataFilePath, true);
          logger.debug("end delete {}", fileName);
        }
      }
    } finally {
      fs.close();
    }
    // Clean up the data structure. Remove unwanted windows' data
    Iterator<Long> windows = deleteSnapshot.keySet().iterator();
    while (windows.hasNext()) {
      if (windows.next() <= windowId) {
        windows.remove();
      }
    }
  }

  /**
   * Takes a snapshot of the deleted files. These files were marked as deleted in the data structures. The files
   * indicated in the snapshot (for the corresponding window) will be deleted in the committed call back.
   *
   * @param windowId
   *          Window id for the window to snapshot
   */
  public void captureFilesToDelete(long windowId)
  {
    if (deletedFiles.isEmpty()) {
      return;
    }
    // Take a snapshot of the files which are marked as delete
    Set<String> filesToDelete = Sets.newHashSet();
    filesToDelete.addAll(deletedFiles);
    deleteSnapshot.put(windowId, filesToDelete);
    deletedFiles.clear();
  }

  /**
   * Does commit window operations for the bucket store. Deletes the files which were recorded as deleted.
   */
  @Override
  public void committed(long windowId)
  {
    final long window = windowId;
    // Process deletes in another thread
    Runnable r = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          processPendingDeletes(window);
        } catch (IOException e) {
          throw new RuntimeException("exception in deleting files", e);
        }
      }
    };
    new Thread(r).start();
  }

  @Override
  public ExpirableHdfsBucketStoreAsync<T> clone() throws CloneNotSupportedException
  {
    return (ExpirableHdfsBucketStoreAsync<T>)super.clone();
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  private static final transient Logger logger = LoggerFactory.getLogger(ExpirableHdfsBucketStoreAsync.class);
}
