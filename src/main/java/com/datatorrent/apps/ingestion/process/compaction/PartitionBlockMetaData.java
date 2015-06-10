/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.AbstractBlockMetadata;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * Interface for partition blocks meta data.
 * {@link PatitionMetaData} will maintain list of partition block meta data objects 
 */
public interface PartitionBlockMetaData 
{
  /** 
   * Buffer size to be used for reading data from block files
   */
  static final int BUFFER_SIZE = 64 * 1024;
  
  /**
   * Write this block to given outputStream
   * @param outputStream
   * @throws IOException 
   */
  public void writeTo(FSDataOutputStream outputStream, FileSystem appFS) throws IOException;
  
  /**
   * Length of the given block
   * @return
   */
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
  public static class FilePartitionBlockMetaData implements PartitionBlockMetaData{

    FileBlockMetadata fileBlockMetadata;
    
    /**
     * Relative path of the source file (w.r.t input directory)
     */
    String sourceRelativePath;
    /**
     * Is this the last block for source file
     */
    boolean isLastBlockSource;
    
    /**
     * Default constructor for serialization
     */
    public FilePartitionBlockMetaData()
    {
    }
    
    /**
     * @param fileBlockMetadata
     * @param sourceRelativePath
     * @param isLastBlockSource
     */
    public FilePartitionBlockMetaData(FileBlockMetadata fileBlockMetadata, String sourceRelativePath, boolean isLastBlockSource)
    {
      super();
      this.fileBlockMetadata = fileBlockMetadata;
      this.sourceRelativePath = sourceRelativePath;
      this.isLastBlockSource = isLastBlockSource;
    }




    /**
     * @return the fileBlockMetadata
     */
    public FileBlockMetadata getFileBlockMetadata()
    {
      return fileBlockMetadata;
    }
    
    /**
     * @param fileBlockMetadata the fileBlockMetadata to set
     */
    public void setFileBlockMetadata(FileBlockMetadata fileBlockMetadata)
    {
      this.fileBlockMetadata = fileBlockMetadata;
    }
    
    /**
     * @return the sourceRelativePath
     */
    public String getSourceRelativePath()
    {
      return sourceRelativePath;
    }
    
    /**
     * @param sourceRelativePath the sourceRelativePath to set
     */
    public void setSourceRelativePath(String sourceRelativePath)
    {
      this.sourceRelativePath = sourceRelativePath;
    }
    
    /**
     * @return the isLastBlockSource
     */
    public boolean isLastBlockSource()
    {
      return isLastBlockSource;
    }
    
    /**
     * @param isLastBlockSource the isLastBlockSource to set
     */
    public void setLastBlockSource(boolean isLastBlockSource)
    {
      this.isLastBlockSource = isLastBlockSource;
    }
    
    /*
     * (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#write(org.apache.hadoop.fs.FSDataOutputStream)
     */
    @Override
    public void writeTo(FSDataOutputStream outputStream, FileSystem appFS) throws IOException
    {
      DataInputStream inStream = null;
      try {
        byte[] buffer = new byte[BUFFER_SIZE];
        int inputBytesRead;
        Path blockPath = new Path(this.fileBlockMetadata.getFilePath());
        if (!appFS.exists(blockPath)) {
          throw new RuntimeException("Exception: Missing block " + blockPath);
        }
        inStream = new DataInputStream(appFS.open(blockPath));
        inStream.skip(this.fileBlockMetadata.getOffset());

        long bytesRemainingToRead = this.getLength();
        int bytesToread = Math.min(BUFFER_SIZE, (int) bytesRemainingToRead);
        while (((inputBytesRead = inStream.read(buffer, 0, bytesToread)) != -1) && bytesRemainingToRead > 0) {
          outputStream.write(buffer, 0, inputBytesRead);
          bytesRemainingToRead -= inputBytesRead;
          bytesToread = Math.min(BUFFER_SIZE, (int) bytesRemainingToRead);
        }
      } finally {
        if (inStream != null) {
          inStream.close();
        }
      }
      
    }




    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#getLength()
     */
    @Override
    public long getLength()
    {
      return fileBlockMetadata.getLength();
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
      FileBlockMetadata leftFileBlockMetaData = new FileBlockMetadata(fileBlockMetadata.getFilePath(), 
          fileBlockMetadata.getBlockId(), fileBlockMetadata.getOffset(), splitOffset, 
          fileBlockMetadata.isLastBlock(), fileBlockMetadata.getPreviousBlockId());

      subBlocks[0] = new FilePartitionBlockMetaData(leftFileBlockMetaData, sourceRelativePath, isLastBlockSource);

      FileBlockMetadata rightFileBlockMetaData = new FileBlockMetadata(fileBlockMetadata.getFilePath(), 
          fileBlockMetadata.getBlockId(), fileBlockMetadata.getOffset() + splitOffset, 
          fileBlockMetadata.getLength() - splitOffset, fileBlockMetadata.isLastBlock(), 
          fileBlockMetadata.getPreviousBlockId());

      subBlocks[1] = new FilePartitionBlockMetaData(rightFileBlockMetaData, sourceRelativePath, isLastBlockSource);
      return subBlocks;
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
    public void writeTo(FSDataOutputStream outputStream, FileSystem appFS) throws IOException
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
