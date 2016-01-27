package com.datatorrent.contrib.hdht;

import java.util.Comparator;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

public class RangeSetTest
{
  @Test
  public void testRangeSet()
  {
    RangeSet<Integer> rset = new RangeSet<>(new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return o1 - o2;
      }
    });

    rset.add(1, 10);
    rset.add(11, 20);
    rset.add(1, 5);

    Assert.assertEquals("Range contains only two items ", 2, rset.ranges.size());

    rset.add(5, 12);

    Assert.assertEquals("Range contains only one item ", 1, rset.ranges.size());
    Assert.assertEquals("Range starts from one ", new Integer(1), rset.iterator().next().start);
    Assert.assertEquals("Range ends at 20 ", new Integer(20), rset.iterator().next().end);

  }

  @Test
  public void testRangeOverlap()
  {
    RangeSet<Integer> rset = new RangeSet<>(new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return o1 - o2;
      }
    });

    rset.add(1, 5);
    rset.add(11, 20);
    rset.add(30, 60);

    /* overlap at start */
    RangeSet<Integer> result = rset.getOverlappingRanges(new Range<>(0, 3));
    Assert.assertEquals("Just one resule returned ", 1, result.ranges.size());
    Assert.assertEquals("Range starts from one ", new Integer(1), result.iterator().next().start);
    Assert.assertEquals("Range ends at 20 ", new Integer(5), result.iterator().next().end);

    /* partial overlap with two ranges */
    result = rset.getOverlappingRanges(new Range<>(0, 25));
    Assert.assertEquals("Just one resule returned ", 2, result.ranges.size());
    Iterator<Range<Integer>> iter = result.iterator();
    Range<Integer> r = iter.next();
    Assert.assertEquals("Range starts from one ", new Integer(1), r.start);
    Assert.assertEquals("Range ends at 20 ", new Integer(5), r.end);
    r = iter.next();
    Assert.assertEquals("Range starts from one ", new Integer(11), r.start);
    Assert.assertEquals("Range ends at 20 ", new Integer(20), r.end);


    /* overlap at end */
    /* overlap at start */
    result = rset.getOverlappingRanges(new Range<>(15, 25));
    Assert.assertEquals("Just one resule returned ", 1, result.ranges.size());
    Assert.assertEquals("Range starts from one ", new Integer(11), result.iterator().next().start);
    Assert.assertEquals("Range ends at 20 ", new Integer(20), result.iterator().next().end);

    result = rset.getOverlappingRanges(new Range<>(6, 29));
    Assert.assertEquals("Just one resule returned ", 1, result.ranges.size());
    Assert.assertEquals("Range starts from one ", new Integer(11), result.iterator().next().start);
    Assert.assertEquals("Range ends at 20 ", new Integer(20), result.iterator().next().end);

    result = rset.getOverlappingRanges(new Range<>(6, 15));
    Assert.assertEquals("Just one resule returned ", 1, result.ranges.size());
    Assert.assertEquals("Range starts from one ", new Integer(11), result.iterator().next().start);
    Assert.assertEquals("Range ends at 20 ", new Integer(20), result.iterator().next().end);

  }
}
