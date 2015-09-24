/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>ExpirableHdfsBucketStore class.</p>
 *
 * @param <T>
 * @since 0.9.5
 */
public class ExpirableHdfsBucketStore<T> extends HdfsBucketStore<T> implements BucketStore.ExpirableBucketStore<T>
{
  Set<String> recordedFiles;
  List<Set<String>> filesToDelete;

  @Override
  public void setup()
  {
    recordedFiles = Sets.newHashSet();
    filesToDelete = Lists.newArrayList();
    super.setup();
  }

  @Override
  public void deleteExpiredBuckets() throws IOException
  {
    logger.debug("Deleting all recorded windows");

    if(filesToDelete.isEmpty()) return;
    Set<String> temp = Sets.newHashSet();
    temp.addAll(filesToDelete.get(0));
    Iterator<String> fileNameItr = temp.iterator();
    FileSystem fs = FileSystem.newInstance(configuration);
    try {
      while(fileNameItr.hasNext()) {
        String fileName = fileNameItr.next();
        Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + fileName);
        if (fs.exists(dataFilePath)) {
          fs.delete(dataFilePath, true);
          filesToDelete.get(0).remove(fileName);
          logger.debug("deleted file {}", fileName);
        }
      }
    }
    finally {
      fs.close();
    }
    filesToDelete.remove(0);
  }

  @Override
  public void deleteBucket(int bucketIdx) throws IOException
  {
    Map<String, Long> offsetMap = bucketPositions[bucketIdx];
    if (offsetMap != null) {
      for (String window : offsetMap.keySet()) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          boolean elementRemoved = indices.remove(bucketIdx);
          if (indices.isEmpty() && elementRemoved) {
            windowToBuckets.removeAll(window);
            windowToTimestamp.remove(window);
            recordedFiles.add(window);
          }
        }
      }
    }
    bucketPositions[bucketIdx] = null;
  }

  @Override
  public void recordAndMarkFilesToDelete(long time)
  {
    logger.debug("Recording all windows < time: {}", time);
    Iterator<String> iterator = windowToBuckets.keySet().iterator();
    for (; iterator.hasNext(); ) {
      String window = iterator.next();
      long timestamp= windowToTimestamp.get(window);
      if (timestamp < time) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          if (indices.size() > 0) {
            recordedFiles.add(window);
            logger.debug("Recording file {}", window);
            for (int bucketIdx : indices) {
              Map<String, Long> offsetMap = bucketPositions[bucketIdx];
              if (offsetMap != null) {
                synchronized (offsetMap) {
                  offsetMap.remove(window);
                  logger.debug("Removing from bucket positions {}", bucketIdx);
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

  @Override
  public void checkpointed(){
    Set<String> temp = Sets.newHashSet();
    temp.addAll(recordedFiles);
    filesToDelete.add(temp);
    recordedFiles.clear();
  }

  @Override
  public ExpirableHdfsBucketStore<T> clone() throws CloneNotSupportedException
  {
    return (ExpirableHdfsBucketStore<T>)super.clone();
  }

  private static transient final Logger logger = LoggerFactory.getLogger(ExpirableHdfsBucketStore.class);

}