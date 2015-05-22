package com.datatorrent.apps.ingestion.io;

import java.io.IOException;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.ingestion.Application.Scheme;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.malhar.lib.io.block.BlockMetadata;
import com.datatorrent.malhar.lib.io.block.FSSliceReader;

@StatsListener.DataQueueSize
public class BlockReader extends FSSliceReader
{
  protected int maxRetries;
  protected Queue<FailedBlock> failedQueue;

  protected Scheme scheme;
  /**
   * maximum number of bytes read per second
   */
  protected long bandwidth;

  @OutputPortFieldAnnotation(optional = true, error = true)
  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> error = new DefaultOutputPort<BlockMetadata.FileBlockMetadata>();

  public BlockReader()
  {
    super();
    this.scheme = Scheme.HDFS;
    maxRetries = 0;
    failedQueue = Lists.newLinkedList();
  }
  
  public BlockReader(Scheme scheme)
  {
    this();
    this.scheme = scheme;
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    switch (scheme) {
    case HDFS:
      return super.getFSInstance();
    case FILE:
      return FileSystem.newInstanceLocal(configuration);
    default:
      throw new UnsupportedOperationException(scheme + " not supported");
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (!failedQueue.isEmpty()) {
      FailedBlock failedBlock = failedQueue.poll();
      failedBlock.retries++;
      processBlockMetadata(failedBlock);
    }
    else {
      super.handleIdleTime();
    }
  }

  @Override
  protected void processBlockMetadata(BlockMetadata.FileBlockMetadata block)
  {
    if (maxRetries == 0) {
      super.processBlockMetadata(block);
    }
    else {
      try {
        super.processBlockMetadata(block);
      }
      catch (Throwable t) {
        if (block instanceof FailedBlock) {
          //A failed block was being processed
          FailedBlock failedBlock = (FailedBlock) block;
          LOG.debug("attempt {} to process block {} failed", failedBlock.retries, failedBlock.block.getBlockId());
          if (failedBlock.retries < maxRetries) {
            failedQueue.add(failedBlock);
          }
          else if (error.isConnected()) {
            error.emit(failedBlock.block);
          }
        }
        else {
          LOG.debug("failed to process block {}", block.getBlockId());
          failedQueue.add(new FailedBlock(block));
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockReader.class);

  protected static class FailedBlock extends BlockMetadata.FileBlockMetadata
  {
    protected int retries;
    protected final FileBlockMetadata block;

    @SuppressWarnings("unused")
    private FailedBlock()
    {
      //for kryo
      block = null;
    }

    FailedBlock(BlockMetadata.FileBlockMetadata block)
    {
      this.block = block;
    }

    @Override
    public long getBlockId()
    {
      return block.getBlockId();
    }
  }

  /**
   * Sets the max number of retries.
   *
   * @param maxRetries maximum number of retries
   */
  public void setMaxRetries(int maxRetries)
  {
    this.maxRetries = maxRetries;
  }

  /**
   * @return the max number of retries.
   */
  public int getMaxRetries()
  {
    return this.maxRetries;
  }

  public void setScheme(String scheme)
  {
    this.scheme = Scheme.valueOf(scheme.toUpperCase());
  }

  public String getScheme()
  {
    return this.scheme.toString();
  }

  public long getBandwidth()
  {
    return this.bandwidth;
  }

  public void setBandwidth(long bandwidth)
  {
    this.bandwidth = bandwidth;
  }

  int getOperatorId()
  {
    return operatorId;
  }

  Set<Integer> getPartitionKeys()
  {
    return this.partitionKeys;
  }

  void setPartitionKeys(Set<Integer> partitionKeys)
  {
    this.partitionKeys = partitionKeys;
  }

  int getPartitionMask()
  {
    return this.partitionMask;
  }

  void setPartitionMask(int partitionMask)
  {
    this.partitionMask = partitionMask;
  }

  BasicCounters<MutableLong> getCounters()
  {
    return this.counters;
  }
}
