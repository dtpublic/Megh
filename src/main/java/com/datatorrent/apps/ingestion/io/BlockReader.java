package com.datatorrent.apps.ingestion.io;

import java.io.IOException;
import java.util.Queue;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;

public class BlockReader extends FSSliceReader
{
  @NotNull
  protected String directory; // Same as FileSpiltter

  protected int maxRetries;
  protected Queue<FailedBlock> failedQueue;

  /**
   * maximum number of bytes read per second
   */
  protected long maxThroughput;

  @OutputPortFieldAnnotation(optional = true, error = true)
  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> error = new DefaultOutputPort<BlockMetadata.FileBlockMetadata>();

  public BlockReader()
  {
    super();
    maxRetries = 0;
    failedQueue = Lists.newLinkedList();
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(new Path(directory).toUri(), configuration);
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

  @Override
  public void endWindow()
  {
    super.endWindow();
  }

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
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

  public long getMaxThroughput()
  {
    return this.maxThroughput;
  }

  public void setMaxThroughput(long maxThroughput)
  {
    this.maxThroughput = maxThroughput;
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
