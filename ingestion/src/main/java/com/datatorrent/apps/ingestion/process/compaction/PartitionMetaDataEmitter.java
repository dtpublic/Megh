/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.FilePartitionBlockMetaData;
import com.datatorrent.apps.ingestion.process.compaction.PartitionBlockMetaData.StaticStringBlockMetaData;
import com.datatorrent.lib.io.output.BlockWriter;
import com.datatorrent.lib.io.output.IdleWindowCounter;
import com.datatorrent.lib.io.output.OutputFileMetaData;
import com.datatorrent.lib.io.output.Synchronizer;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputFileBlockMetaData;

/**
 * An operator used in compaction for generating partition meta data. Partition meta data defines list of
 * FileBlockMetadata which constitutes one partition file.
 *
 * @since 1.0.0
 */
public class PartitionMetaDataEmitter extends IdleWindowCounter
{

  /**
   * Name of the compactionBundle. 
   * By default application name will be used as bundle name
   */
  protected String compactionBundleName;

  /** Format for name of the partition files */
  protected String partFileNameFormat = "%s_%d.partition"; // compactionBundleName_partitionNo.partition

  /** Size of each partition */
  protected long partitionSizeInBytes; 

  /** File boundaries within the partition will be seperated by this symbol */
  protected String fileBoundarySeperator;

  /** Current partition ID */
  private long currentPartitionID;

  /** No of bytes filled in current partition */
  private long noBytesInCurrentPartition;

  /** Queue holding blocks to be written */
  private List<PartitionBlockMetaData> blocksInProgress;

  /** Maintains blocks for the current part file */
  private List<PartitionBlockMetaData> blocksForCurrentPartFile;

  /** Path to directory for storing intermediate blocks */
  protected transient String blocksPath;

  protected transient FileSystem appFS;
  
  private static final int COMPACTION_IDLE_WINDOWS_THRESHOLD_DEFAULT = 600;

  public PartitionMetaDataEmitter()
  {
    blocksForCurrentPartFile = Lists.newArrayList();
    blocksInProgress = Lists.newLinkedList();
    currentPartitionID = 0;
    noBytesInCurrentPartition = 0;
  }

  /*
   * (non-Javadoc)
   * Initializes appFS and blocksPath
   * @see com.datatorrent.api.BaseOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      blocksPath = context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
      compactionBundleName = context.getValue(Context.DAGContext.APPLICATION_NAME);
      appFS = FileSystem.newInstance((new Path(blocksPath)).toUri(), new Configuration());
    } catch (IOException ex) {
      LOG.error("Exception in PartitionMetaDataEmitter setup.", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Input port accepts {@link FileMetadata} from {@link Synchronizer} which indicates that all blocks for that file are
   * available under appFS
   * 
   */
  public final transient DefaultInputPort<FileMetadata> processedFileInput = new DefaultInputPort<FileMetadata>() {
    @Override
    public void process(FileMetadata fileMetadata)
    {
      markActivity();
      LOG.debug("Received {}", fileMetadata.getFileName());
      try {
        processSourceFile(fileMetadata);
      } catch (IOException e) {
        throw new RuntimeException("Exception in handling "+ fileMetadata.getFileName(), e);
      }
    }
  };

  /**
   * Merge/Split file specified in {@link FileMetadata} into partition
   * 
   * @param fPath
   *          File Path,
   * @throws IOException
   */
  private void processSourceFile(FileMetadata fileMetadata) throws IOException
  {
    if (fileMetadata instanceof IngestionFileMetaData) {
      IngestionFileMetaData ingestionFileMetaData = (IngestionFileMetaData) fileMetadata;
      FilePartitionInfo filePartitionInfo = new FilePartitionInfo(
          ingestionFileMetaData, currentPartitionID, noBytesInCurrentPartition);
      
      List<PartitionBlockMetaData> blockMetaDataList = populateFileBlocksInfo(filePartitionInfo);
      blocksInProgress.addAll(blockMetaDataList);

      while (!blocksInProgress.isEmpty()) {
        long noBytesCanBePushedToCurrentPartition = partitionSizeInBytes - noBytesInCurrentPartition;
        LOG.debug("noBytesCanBePushedToCurrentPartition={}", noBytesCanBePushedToCurrentPartition);
        
        PartitionBlockMetaData block = blocksInProgress.remove(0);
        if (block.getLength() <= noBytesCanBePushedToCurrentPartition) {
          // Enough space available in current part file
          LOG.debug("Enough space available in current part file");
          blocksForCurrentPartFile.add(block);
          noBytesInCurrentPartition += block.getLength();
          if ( (block instanceof OutputFileBlockMetaData) && ((OutputFileBlockMetaData)block).isLastBlockSource()) {
            filePartitionInfo.endPartitionId = currentPartitionID;
            filePartitionInfo.endOffset = noBytesInCurrentPartition;
            filePartitionInfoOutputPort.emit(filePartitionInfo);
            LOG.debug("Emiting filePartitionInfo for {} = {}:{}-{}:{}", 
                filePartitionInfo.ingestionFileMetaData.getRelativePath(),
                filePartitionInfo.startPartitionId,
                filePartitionInfo.startOffset,
                filePartitionInfo.endPartitionId,
                filePartitionInfo.endOffset);
          }
          
          if(partitionSizeInBytes == noBytesInCurrentPartition){
            commitPartFile();
          }
        } else {
          // Not enough space available in current part file to accommodate complete block
          LOG.debug("Not enough space available in current part file to accommodate complete block");
          if (noBytesCanBePushedToCurrentPartition > 0) {
            // some space available to accommodate partial block
            PartitionBlockMetaData[] blockSplits = block.split(noBytesCanBePushedToCurrentPartition);
            blocksForCurrentPartFile.add(blockSplits[0]);
            blocksInProgress.add(0, blockSplits[1]);
          }
          commitPartFile();
        }
      }
    } else {
      throw new IllegalArgumentException("Expecting instance of IngestionFileMetaData");
    }
  }

  /**
   * Flush out the meta data for last partition
   */
  @Override
  public void teardown()
  {
    flushBlocksInProgress();
  }
  
  
  /** 
   * No additional check required for counting idle windows
   * @see com.datatorrent.lib.io.output.IdleWindowCounter#hasMoreWork()
   */
  @Override
  protected boolean hasMoreWork()
  {
    return false;
  }
  
  /**
   * Commit current partition if idle window threshold is reached
   * @see com.datatorrent.lib.io.output.IdleWindowCounter#idleWindowThresholdReached()
   */
  @Override
  protected void idleWindowThresholdReached()
  {
    flushBlocksInProgress();
  }
  
  protected void flushBlocksInProgress(){
    if(blocksInProgress.size() + blocksForCurrentPartFile.size() > 0){
      blocksForCurrentPartFile.addAll(blocksInProgress);
      blocksInProgress.clear();
      commitPartFile();
    }
  }
  
  @Override
  protected int getIdleWindowThresholdDefault()
  {
    return COMPACTION_IDLE_WINDOWS_THRESHOLD_DEFAULT;
  }

  /**
   * Populate file block information from fileMetadata.
   * This assumes that all blocks for a file are available under blocksPath
   * 
   * @param fileMetadata
   * @return
   * @throws IOException
   */
  private List<PartitionBlockMetaData> populateFileBlocksInfo(FilePartitionInfo filePartitionInfo) throws IOException
  {
    List<PartitionBlockMetaData> blockMetadataList = Lists.newArrayList();
    if(filePartitionInfo.ingestionFileMetaData.isDirectory()){
      filePartitionInfo.endPartitionId = currentPartitionID;
      filePartitionInfo.endOffset = noBytesInCurrentPartition;
      filePartitionInfoOutputPort.emit(filePartitionInfo);
      return blockMetadataList;
    }
    IngestionFileMetaData fileMetadata = filePartitionInfo.ingestionFileMetaData;
    long previousBlockId = -1;
    for (int i = 0; i < fileMetadata.getBlockIds().length; i++) {
      long blockId = fileMetadata.getBlockIds()[i];
      Path blockFilePath = new Path(blocksPath, Long.toString(blockId));
      long length = appFS.getFileStatus(blockFilePath).getLen();
      boolean isLastBlockSource = (i == fileMetadata.getBlockIds().length - 1);
      FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(blockFilePath.toString(), blockId, 0L, length, false, previousBlockId);
      PartitionBlockMetaData fileBlock = new FilePartitionBlockMetaData(fileBlockMetadata, fileMetadata.getRelativePath(), isLastBlockSource);
      blockMetadataList.add(fileBlock);
      previousBlockId = blockId;
    }
    
    //Add static string block for file separator
    StaticStringBlockMetaData separatorBlock = new StaticStringBlockMetaData(fileBoundarySeperator);
    blockMetadataList.add(separatorBlock);
    return blockMetadataList;
  }

  /**
   * Emits metadata for one partition. Information about which blocksForCurrentPartFile
   */
  private void commitPartFile()
  {
    String partFileName = String.format(partFileNameFormat, compactionBundleName, currentPartitionID);
    PatitionMetaData patitionMetaData = new PatitionMetaData(currentPartitionID, partFileName, blocksForCurrentPartFile);
    patitionMetaDataOutputPort.emit(patitionMetaData);
    LOG.debug("Emiting patitionMetaData={} id={}", patitionMetaData, currentPartitionID);
    blocksForCurrentPartFile = Lists.newArrayList();
    noBytesInCurrentPartition = 0;
    ++currentPartitionID;
  }
  
  /**
   * @return the compactionBundleName
   */
  public String getCompactionBundleName()
  {
    return compactionBundleName;
  }
  
  /**
   * @param compactionBundleName the compactionBundleName to set
   */
  public void setCompactionBundleName(String compactionBundleName)
  {
    this.compactionBundleName = compactionBundleName;
  }

  /**
   * @return the partitionSizeInBytes
   */
  public long getPartitionSizeInBytes()
  {
    return partitionSizeInBytes;
  }
  
  
  /**
   * @param partitionSizeInBytes the partitionSizeInBytes to set
   */
  public void setPartitionSizeInBytes(long partitionSizeInBytes)
  {
    this.partitionSizeInBytes = partitionSizeInBytes;
  }
  
  
  /**
   * @return the fileBoundarySeperator
   */
  public String getFileBoundarySeperator()
  {
    return fileBoundarySeperator;
  }
  
  /**
   * @param fileBoundarySeperator the fileBoundarySeperator to set
   */
  public void setFileBoundarySeperator(String fileBoundarySeperator)
  {
    this.fileBoundarySeperator = fileBoundarySeperator;
  }

  
  /**
   * Block meta data with additional information about source file.
   */
  
  
  /**
   * Data structure used for emiting meta data about contents of partition.
   */
  public static class PatitionMetaData implements OutputFileMetaData
  {
    /**
     * Unique id of the partition.
     * partitionIDs are w.r.t. compactionBundle.
     * partitionID starts with zero and are auto incremented.
     */
    private long partitionID = 1;
    
    /**
     * Name of the partition file.
     */
    private String partFileName;
    
    /**
     * List of blocks stored under this partition.
     */
    List<PartitionBlockMetaData> blockMetaDataList;

    public PatitionMetaData()
    {
      
    }
    
    public PatitionMetaData(long partitionID, String partFileName, List<PartitionBlockMetaData> blockMetaDataList)
    {
      this.partitionID = partitionID;
      this.partFileName = partFileName;
      this.blockMetaDataList = blockMetaDataList;
    }

    /**
     * @return the partitionID
     */
    public long getPartitionID()
    {
      return partitionID;
    }

    /**
     * @return the partFileName
     */
    public String getPartFileName()
    {
      return partFileName;
    }

    /**
     * @return the blockMetaDataList
     */
    public List<PartitionBlockMetaData> getBlockMetaDataList()
    {
      return blockMetaDataList;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData#getOutputRelativePath()
     */
    @Override
    public String getOutputRelativePath()
    {
      // TODO Auto-generated method stub
      return partFileName;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData#getOutputBlocks()
     */
    @Override
    public List<OutputBlock> getOutputBlocksList()
    {
      return new ArrayList<OutputFileMetaData.OutputBlock> (blockMetaDataList);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(PartitionMetaDataEmitter.class);
  
  /**
   * Output port emiting one tuple for each partition to be created.
   * Tuple is emited only after sufficient data is available to occupy entire partition (specified by partitionSizeInBytes).
   * 
   */
  public final transient DefaultOutputPort<PatitionMetaData> patitionMetaDataOutputPort = new DefaultOutputPort<PatitionMetaData>();
  
  /**
   * Output port emiting one tuple for each file indicating partitions spanned by that file. 
   */
  public final transient DefaultOutputPort<FilePartitionInfo> filePartitionInfoOutputPort = new DefaultOutputPort<FilePartitionInfo>();
  

  /**
   * File meta data with additional information about start, end partition:offset
   */
  public static class FilePartitionInfo
  {
    IngestionFileMetaData ingestionFileMetaData;
    /**
     * ID of the partition where file begins. 
     */
    long startPartitionId;
    /**
     * ID of the partition where file ends.
     * In case if file does not span multiple partitions then endPartitionId will be same as startPartitionId
     */
    long endPartitionId;
    
    /**
     * Offsets are starting with 0. Start is inclusive. Start offset = k indicates that; start reading from k'th byte
     * (inclusive). E.g. Start offset=100 indicates that skip bytes b0...b99. Start reading from b100.
     * */
    long startOffset;

    /**
     * Offsets are starting with 0. End is exclusive. End offset = k indicates that; stop reading at k-1'th byte
     * (exclusive). E.g. End offset=200 indicates that read till byte b199.
     * */
    long endOffset;

    /*Default constructor needed for serialization*/
    public FilePartitionInfo()
    {
    }

    public FilePartitionInfo(IngestionFileMetaData ingestionFileMetaData, long startPartitionId, long startOffset)
    {
      this.ingestionFileMetaData = ingestionFileMetaData;
      this.startPartitionId = startPartitionId;
      this.startOffset = startOffset;
    }

    /**
     * @return the startPartitionId
     */
    public long getStartPartitionId()
    {
      return startPartitionId;
    }

    /**
     * @param startPartitionId the startPartitionId to set
     */
    public void setStartPartitionId(long startPartitionId)
    {
      this.startPartitionId = startPartitionId;
    }

    /**
     * @return the endPartitionId
     */
    public long getEndPartitionId()
    {
      return endPartitionId;
    }

    /**
     * @param endPartitionId the endPartitionId to set
     */
    public void setEndPartitionId(long endPartitionId)
    {
      this.endPartitionId = endPartitionId;
    }

    /**
     * @return the startOffset
     */
    public long getStartOffset()
    {
      return startOffset;
    }

    /**
     * @param startOffset the startOffset to set
     */
    public void setStartOffset(long startOffset)
    {
      this.startOffset = startOffset;
    }

    /**
     * @return the endOffset
     */
    public long getEndOffset()
    {
      return endOffset;
    }

    /**
     * @param endOffset the endOffset to set
     */
    public void setEndOffset(long endOffset)
    {
      this.endOffset = endOffset;
    }
  }

}
