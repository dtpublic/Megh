/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * RangeSet maintains the set of Ranges.
 * This needs an element comparator and keeps the ranges in a TreeSet sorted
 * by start of the range. This supports add, contains, merge operations.
 * 
 * @since 3.3.0
 *
 * @param <T>
 */
class RangeSet<T> implements Iterable<Range<T>>
{
  Comparator<T> cmp;
  TreeSet<Range<T>> ranges;
  Range.RangeComparator<T> rangeCmp;

  public RangeSet(Comparator<T> cmp)
  {
    this.cmp = cmp;
    this.rangeCmp = new Range.RangeComparator<T>(cmp);
    this.ranges = new TreeSet<>(rangeCmp);
  }

  public void add(Range<T> r)
  {
    ranges.add(r);
    merge();
  }

  public void add(T start, T end)
  {
    /* current range */
    Range<T> range = new Range<>(start, end);
    add(range);
  }

  /**
   * Reduce the number of purge entries, by merging them if possible.
   * This is a helper method and is called after new elements are added to the set.
   * Basic algorithm is keep a prev range, which is initially set to the first
   * element of the set. During traversal of the set we will check if we can merge
   * current range with the prev range. If merge is possible then we will extend the prev range
   * else we will add a prev range to set, and set it to the current range.
   *
   * @return
   */
  private void merge()
  {
    Range<T> range = null;
    TreeSet<Range<T>> mset = new TreeSet<>(rangeCmp);
    Iterator<Range<T>> iter = ranges.iterator();
    if (!iter.hasNext()) {
      return;
    }
    range = iter.next();
    while (iter.hasNext()) {
      Range<T> r = iter.next();
      if (cmp.compare(range.end, r.start) >= 0) {
        range = new Range<>(range.start, cmp.compare(range.end, r.end) > 0 ? range.end : r.end);
      } else {
        mset.add(range);
        range = r;
      }
    }
    // add last range in the list.
    mset.add(range);
    ranges = mset;
  }

  public void merge(RangeSet<T> rset)
  {
    ranges.addAll(rset.ranges);
    merge();
  }

  /**
   * check if elem is contains within some range of the set.
   *
   * @param elem
   * @return
   */
  public boolean contains(T elem)
  {
    Range<T> floor = ranges.floor(new Range<>(elem, null));
    return floor.contains(elem, cmp);
  }

  public void addAll(Collection<Range<T>> ranges)
  {
    this.ranges.addAll(ranges);
    merge();
  }

  @Override
  public Iterator<Range<T>> iterator()
  {
    return ranges.iterator();
  }

  public void clear()
  {
    ranges.clear();
  }

  public boolean isEmpty()
  {
    return ranges.isEmpty();
  }

  /**
   * Returns the ranges overlapping with given range.
   *
   * @param range
   * @return
   */
  RangeSet<T> getOverlappingRanges(Range<T> range)
  {
    RangeSet<T> overlapping = new RangeSet<>(cmp);
    for (Range<T> r : ranges) {
      if (r.overlapsWith(range, cmp)) {
        overlapping.add(r);
      }
    }
    return overlapping;
  }

  /**
   * check whether a range is fully contained by any range in the set.
   *
   * @param range
   * @return
   */
  public boolean containsFully(Range<T> range)
  {
    for (Range<T> r : ranges) {
      if (range.subsetOf(r, cmp)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get first point in the ranges.
   * @return
   */
  public T getFirst()
  {
    return ranges.first().start;
  }

  /**
   * Return the largest point in the range.
   * @return
   */
  public T getLast()
  {
    return ranges.last().end;
  }
}
