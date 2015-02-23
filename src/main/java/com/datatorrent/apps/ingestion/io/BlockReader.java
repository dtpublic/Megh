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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;

public class BlockReader extends FSSliceReader
{
  @NotNull
  protected String directory; // Same as FileSpiltter directory.

  protected int maxRetries;
  protected Queue<FailedBlock> failedQueue;

  /**
   * maximum number of bytes read per second
   */
  protected long maxThroughput;

  private transient long timePerWindow;

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
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    counters.setCounter(BlockKeys.READ_TIME_WINDOW, new MutableLong());
  }

  @Override
  protected boolean canHandleMoreBlocks()
  {
    return (!blockQueue.isEmpty() || !failedQueue.isEmpty()) && blocksPerWindow < threshold;
  }

  @Override
  protected void processHeadBlock()
  {
    long start = System.currentTimeMillis();

    if (blockQueue.isEmpty() && !failedQueue.isEmpty()) {
      FailedBlock failedBlock = failedQueue.poll();
      failedBlock.retries++;
      try {
        processBlockMetadata(failedBlock);
      }
      catch (Throwable t) {
        LOG.debug("attempt {} to process block {} failed", failedBlock.retries, failedBlock.block.getBlockId());
        if (failedBlock.retries < maxRetries) {
          failedQueue.add(failedBlock);
        }
        else if (error.isConnected()) {
          error.emit(failedBlock.block);
        }
      }
    }
    else {
      BlockMetadata.FileBlockMetadata head = blockQueue.poll();
      if (maxRetries == 0) {
        processBlockMetadata(head);
      }
      else {
        try {
          processBlockMetadata(head);
        }
        catch (Throwable t) {
          LOG.debug("failed to process block {}", head.getBlockId());
          failedQueue.add(new FailedBlock(head));
        }
      }
    }
    timePerWindow += System.currentTimeMillis() - start;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    counters.getCounter(BlockKeys.READ_TIME_WINDOW).setValue(timePerWindow);
    timePerWindow = 0;
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

  //Methods for supporting custom partitioner
  ImmutableList<BlockMetadata.FileBlockMetadata> getBlocksQueue()
  {
    return ImmutableList.copyOf(blockQueue);
  }

  void clearBlockQueue()
  {
    this.blockQueue.clear();
  }

  void addBlockMetadata(BlockMetadata.FileBlockMetadata blockMetadata)
  {
    this.blockQueue.add(blockMetadata);
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

  public static enum BlockKeys
  {
    READ_TIME_WINDOW
  }

}
