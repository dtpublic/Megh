/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.apps.ingestion.common.BlockNotFoundException;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputFileBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.AbstractBlockMetadata;

/**
 * Interface for partition blocks meta data.
 * {@link PatitionMetaData} will maintain list of partition block meta data objects
 *
 * @since 1.0.0
 */
public interface PartitionBlockMetaData extends OutputBlock

{
  /** 
   * Buffer size to be used for reading data from block files
   */
  static final int BUFFER_SIZE = 64 * 1024;
  

  public long getOffset();
  public long getLength();
  
  /**
   * Split this block at given splitOffset 
   * @param splitOffset
   * @return array consisting of 2 sub-blocks
   */
  public PartitionBlockMetaData[] split(long splitOffset);
  
  /**
   * Partition block representing chunk of data from block files
   */
  public static class FilePartitionBlockMetaData extends OutputFileBlockMetaData implements PartitionBlockMetaData{

    /**
     * 
     */
    public FilePartitionBlockMetaData()
    {
      // TODO Auto-generated constructor stub
    }

    /**
     * @param leftFileBlockMetaData
     * @param sourceRelativePath
     * @param lastBlockSource
     */
    public FilePartitionBlockMetaData(FileBlockMetadata fmd, String sourceRelativePath, boolean lastBlockSource)
    {
      super(fmd, sourceRelativePath, lastBlockSource);
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#split(long)
     */
    @Override
    public PartitionBlockMetaData[] split(long splitOffset)
    {
      FilePartitionBlockMetaData[] subBlocks = new FilePartitionBlockMetaData[2];
      FileBlockMetadata leftFileBlockMetaData = new FileBlockMetadata(this.getFilePath(), 
          this.getBlockId(), this.getOffset(), splitOffset, 
          this.isLastBlock(), this.getPreviousBlockId());

      subBlocks[0] = new FilePartitionBlockMetaData(leftFileBlockMetaData, getSourceRelativePath(), isLastBlockSource());

      FileBlockMetadata rightFileBlockMetaData = new FileBlockMetadata(this.getFilePath(), 
          this.getBlockId(), this.getOffset() + splitOffset, 
          this.getLength() - splitOffset, this.isLastBlock(), 
          this.getPreviousBlockId());

      subBlocks[1] = new FilePartitionBlockMetaData(rightFileBlockMetaData, getSourceRelativePath(), isLastBlockSource());

      return subBlocks;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData.OutputFileBlockMetaData#writeTo(org.apache.hadoop.fs.FileSystem, java.lang.String, java.io.OutputStream)
     */
    @Override
    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream) throws IOException, BlockNotFoundException
    {
      super.writeTo(appFS, blocksDir, outputStream, getOffset(), getLength());
    }
    
  }
  
  /**
   * This class represents block consisting of static strings like fileseperator.
   */
  public static class StaticStringBlockMetaData extends AbstractBlockMetadata implements PartitionBlockMetaData{
    private String contents;
    private byte[] contentBytes;
    /**
     * Default constructor for serialization
     */
    public StaticStringBlockMetaData()
    {
      
    }
    
    /**
     * @param contents
     */
    public StaticStringBlockMetaData(String contents)
    {
      this(contents, 0L, contents.length());
    }
    
    /**
     * @param contents
     */
    public StaticStringBlockMetaData(String contents, long offset, long length)
    {
      super(offset, length, false, -1);
      this.contents = contents;
      this.contentBytes = contents.getBytes();
    }
    
    
    /**
     * @param contents the contents to set
     */
    public void setContents(String contents)
    {
      this.contents = contents;
    }
    
    /**
     * @return the contents
     */
    public String getContents()
    {
      return contents;
    }
    
    /**
     * @param contentBytes the contentBytes to set
     */
    public void setContentBytes(byte[] contentBytes)
    {
      this.contentBytes = contentBytes;
    }
    
    
    /**
     * @return the contentBytes
     */
    public byte[] getContentBytes()
    {
      return contentBytes;
    }
    
    /**
     * 
     * @see com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#write(org.apache.hadoop.fs.FSDataOutputStream)
     * @param outputStream outputStream to write to
     * @param appFS ignored. Argument just to match signature with the interface
     *  
     */
    @Override
    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream) throws IOException
    {
      outputStream.write(contentBytes, (int)this.getOffset(), (int)this.getLength());
    }

    /**
     * Blockid is not relevant for this implementation.
     * @return dummy block id
     */
    @Override
    public long getBlockId()
    {
      // TODO Auto-generated method stub
      return -1;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#split(long)
     */
    @Override
    public PartitionBlockMetaData[] split(long splitOffset)
    {
      StaticStringBlockMetaData [] subBlocks = new StaticStringBlockMetaData[2];
      subBlocks[0] = new StaticStringBlockMetaData(contents, getOffset(), splitOffset);
      subBlocks[1] = new StaticStringBlockMetaData(contents, getOffset() + splitOffset, getLength() - splitOffset);
      
      return subBlocks;
    }
  }
}
