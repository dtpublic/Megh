package com.datatorrent.apps.ingestion.lib;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.apps.ingestion.io.BandwidthLimitingInputOperator;
import com.datatorrent.common.partitioner.StatelessPartitioner;

public class BandwidthPartitioner<T extends BandwidthLimitingInputOperator> extends StatelessPartitioner<T>
{
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(BandwidthPartitioner.class);

  /**
   * This creates a partitioner which creates only one partition.
   */
  public BandwidthPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   * 
   * @param value
   *          A string which is an integer of the number of partitions to create
   */
  public BandwidthPartitioner(String value)
  {
    super(value);
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   * 
   * @param partitionCount
   *          The number of partitions to create.
   */
  public BandwidthPartitioner(int partitionCount)
  {
    super(partitionCount);
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
  {
    long currentBandwidth = partitions.iterator().next().getPartitionedInstance().getBandwidthManager().getBandwidth() * partitions.size();
    Collection<Partition<T>> newpartitions = super.definePartitions(partitions, context);
    return updateBandwidth(newpartitions, currentBandwidth);
  }

  public Collection<Partition<T>> updateBandwidth(Collection<Partition<T>> newpartitions, long currentBandwidth)
  {
    LOG.info("Updating bandwidth of partitions.");
    long newBandwidth = currentBandwidth / newpartitions.size();
    for (Partition<T> partition : newpartitions) {
      partition.getPartitionedInstance().getBandwidthManager().setBandwidth(newBandwidth);
    }
    return newpartitions;
  }
}
