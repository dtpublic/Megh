package com.datatorrent.apps.ingestion.lib;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingInputOperator;
import com.google.common.collect.Lists;

public class BandwidthPartitionerTest
{
  @Mock
  private BandwidthManager bandwidthManagerMock;
  @Mock
  private BandwidthLimitingInputOperator operatorMock;
  @Mock
  private Partition<BandwidthLimitingInputOperator> partitionMock;
  @Mock
  private Partitioner.PartitioningContext partitionContextMock;
  @Mock
  private Iterator<Partition<BandwidthLimitingInputOperator>> iteratorMock;
  private BandwidthPartitioner<BandwidthLimitingInputOperator> underTest = new BandwidthPartitioner<BandwidthLimitingInputOperator>();

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
    when(iteratorMock.hasNext()).thenReturn(true, false);
    when(iteratorMock.next()).thenReturn(partitionMock);
    when(partitionMock.getPartitionedInstance()).thenReturn(operatorMock);
    when(operatorMock.getBandwidthManager()).thenReturn(bandwidthManagerMock);
    when(bandwidthManagerMock.getBandwidth()).thenReturn(10L);
    when(partitionContextMock.getInputPorts()).thenReturn(null);
  }

  @Test
  public void testBandwidthOnPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(0); // no partitions
    Collection<Partition<BandwidthLimitingInputOperator>> partitions = Lists.newArrayList();
    DefaultPartition<BandwidthLimitingInputOperator> defaultPartition = new DefaultPartition<BandwidthLimitingInputOperator>(operatorMock);
    partitions.add(defaultPartition);

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock).setBandwidth(10L);
  }

  @Test
  public void testBandwidthOnIncresedPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(5);
    Collection<Partition<BandwidthLimitingInputOperator>> partitions = Lists.newArrayList();
    DefaultPartition<BandwidthLimitingInputOperator> defaultPartition = new DefaultPartition<BandwidthLimitingInputOperator>(operatorMock);
    partitions.add(defaultPartition);

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock, times(5)).setBandwidth(2L);
  }

  @Test
  public void testBandwidthOnReducedPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(2);
    when(bandwidthManagerMock.getBandwidth()).thenReturn(2L);
    Collection<Partition<BandwidthLimitingInputOperator>> partitions = Lists.newArrayList();

    for (int i = 5; i-- > 0;) {
      partitions.add(new DefaultPartition<BandwidthLimitingInputOperator>(operatorMock));
    }

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock, times(2)).setBandwidth(5L);
  }

}
