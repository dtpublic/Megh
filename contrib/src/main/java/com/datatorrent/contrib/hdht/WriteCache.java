/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import com.datatorrent.netlet.util.Slice;

/**
 * WriteCache
 * Writecache hold data in memory till it is written out to the data files.
 * This is a HashMap, with functionality to support purge operation.
 *
 * Many write cache can be linked together forming a bigger write cache,
 * these linked cache are separated by purge operations performed between
 * two write caches.
 *
 * put operation will add the data in hashmap.
 *
 * get operation will first search in the hapmap. If data is found it is returned,
 * else it will see if the key is within the purge operations performed on the
 * cache, in this case we will return DELETED for that key. If we link multiple
 * caches then this get operation will prevent reading purged keys from older
 * caches.
 *
 * purge operation will remove keys which falls within purge range from memory,
 * and will note the purge range for get operations.
 */
class WriteCache extends HashMap<Slice, byte[]>
{
  private RangeSet<Slice> purges;
  private Comparator<Slice> cmp;

  public WriteCache(Comparator<Slice> cmp)
  {
    this.cmp = cmp;
  }

  /**
   * Purge operator removes keys falls in purge range from memory, and note down
   * the purge range.
   *
   * @param start
   * @param end
   */
  public void purge(Slice start, Slice end)
  {
    Range<Slice> range = new Range<>(start, end);
    Iterator<Slice> iter = keySet().iterator();
    while (iter.hasNext()) {
      Slice key = iter.next();
      if (range.contains(key, cmp)) {
        iter.remove();
      }
    }
    if (purges == null) {
      purges = new RangeSet<>(cmp);
    }
    purges.add(range);
  }

  /**
   * Merge with other write cache with purge.
   * other contains the more recent data, i.e the new operations performed
   * on the write caches are stored in other. The purge operations stored in
   * other are more recent, they will invalidate the writes done previously.
   * After purge is applied, add all keys to the this set.
   *
   * @param other
   * @return
   */
  public void merge(WriteCache other)
  {
    /**
     * Remove the keys from this map, which are removed
     * by the purge operation done later. (i.e purge operations
     * in other).
     */
    if (other.purges != null) {
      Iterator<Slice> iter = keySet().iterator();
      while (iter.hasNext()) {
        Slice key = iter.next();
        for (Range r : other.purges) {
          if (r.contains(key, cmp)) {
            iter.remove();
          }
        }
      }
    }

    /**
     * merge keys
     */
    putAll(other);
    mergePurgeList(other.purges);
  }

  void mergePurgeList(RangeSet<Slice> rset)
  {
    if (rset == null) {
      return;
    }
    if (this.purges == null) {
      this.purges = new RangeSet<>(cmp);
    }
    this.purges.merge(rset);
  }

  /**
   * Returns data from the cahce if available.
   *
   * In case data is not available in this cache, but it was removed as
   * part of purge operation, it will return DELETED. If we link multiple
   * caches then this check will prevent reading purged keys from older
   * caches.
   *
   * @param key
   * @return
   */
  @Override
  public byte[] get(Object key)
  {
    byte[] data = super.get(key);
    if (data != null) {
      return data;
    }

    // check the purge range, if key is within purge range, then return DELETED.
    if (purges != null) {
      for (Range<Slice> r : purges) {
        if (r.contains((Slice)key, cmp)) {
          return HDHTWriter.DELETED;
        }
      }
    }
    return null;
  }

  /**
   * Take account of purge operations which checking for empty.
   *
   * @return
   */
  @Override
  public boolean isEmpty()
  {
    boolean empty = super.isEmpty();
    // In case where write cache contains only purge operations, return false.
    if (empty) {
      if (purges == null) {
        return true;
      } else {
        return false;
      }
    }
    return empty;
  }

  /**
   * clear in memory data, as well as clear our the purge ranges.
   */
  @Override
  public void clear()
  {
    super.clear();
    if (purges != null) {
      purges.clear();
    }
    purges = null;
  }

  /**
   * Returns purge operations performed on the cache.
   *
   * @return RangeSet<Slice>
   */
  public RangeSet<Slice> getPurges()
  {
    return purges;
  }
}
