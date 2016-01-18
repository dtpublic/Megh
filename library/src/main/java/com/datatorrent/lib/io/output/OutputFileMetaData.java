/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * OutputFileMetaData is used by OutputFileMerger to define constituents of the
 * output file. IngestionFileMetaData and PartitionMetaData are extended from
 * this class.
 *
 */
public interface OutputFileMetaData
{
  /**
   * @return the outputFileName
   */
  public String getOutputRelativePath();

  public List<OutputBlock> getOutputBlocksList();

  public interface OutputBlock
  {
    public void writeTo(FileSystem blocksFS, String blocksDir, OutputStream outputStream)
        throws IOException, BlockNotFoundException;

    public long getBlockId();
  }

  /**
   * Buffer size to be used for reading data from block files
   */
  public static final int BUFFER_SIZE = 64 * 1024;

  /**
   * Partition block representing chunk of data from block files
   */
  public static class OutputFileBlockMetaData extends FileBlockMetadata implements OutputBlock
  {

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
    public OutputFileBlockMetaData()
    {
      super();
    }

    /**
     * @param fileBlockMetadata
     * @param sourceRelativePath
     * @param isLastBlockSource
     */
    public OutputFileBlockMetaData(FileBlockMetadata fmd, String sourceRelativePath, boolean isLastBlockSource)
    {
      super(fmd.getFilePath(), fmd.getBlockId(), fmd.getOffset(), fmd.getLength(), fmd.isLastBlock(),
          fmd.getPreviousBlockId());
      this.sourceRelativePath = sourceRelativePath;
      this.isLastBlockSource = isLastBlockSource;
    }

    /**
     * @return the sourceRelativePath
     */
    public String getSourceRelativePath()
    {
      return sourceRelativePath;
    }

    /**
     * @param sourceRelativePath
     *          the sourceRelativePath to set
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
     * @param isLastBlockSource
     *          the isLastBlockSource to set
     */
    public void setLastBlockSource(boolean isLastBlockSource)
    {
      this.isLastBlockSource = isLastBlockSource;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.datatorrent.apps.ingestion.process.compaction.PartitionBlock#write(org.apache.hadoop.fs.FSDataOutputStream)
     */
    @Override
    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream)
        throws IOException, BlockNotFoundException
    {
      Path blockPath = new Path(blocksDir, Long.toString(getBlockId()));
      if (!appFS.exists(blockPath)) {
        throw new BlockNotFoundException(blockPath);
      }
      writeTo(appFS, blocksDir, outputStream, 0, appFS.getFileStatus(blockPath).getLen());
    }

    public void writeTo(FileSystem appFS, String blocksDir, OutputStream outputStream, long offset, long length)
        throws IOException, BlockNotFoundException
    {
      InputStream inStream = null;

      byte[] buffer = new byte[BUFFER_SIZE];
      int inputBytesRead;
      Path blockPath = new Path(blocksDir, Long.toString(getBlockId()));
      if (!appFS.exists(blockPath)) {
        throw new BlockNotFoundException(blockPath);
      }
      inStream = appFS.open(blockPath);
      try {
        inStream.skip(offset);

        long bytesRemainingToRead = length;
        int bytesToread = Math.min(BUFFER_SIZE, (int)bytesRemainingToRead);
        while (((inputBytesRead = inStream.read(buffer, 0, bytesToread)) != -1) && bytesRemainingToRead > 0) {
          outputStream.write(buffer, 0, inputBytesRead);
          bytesRemainingToRead -= inputBytesRead;
          bytesToread = Math.min(BUFFER_SIZE, (int)bytesRemainingToRead);
        }
      } finally {
        inStream.close();
      }

    }

    private static final Logger LOG = LoggerFactory.getLogger(OutputFileBlockMetaData.class);
  }

}
