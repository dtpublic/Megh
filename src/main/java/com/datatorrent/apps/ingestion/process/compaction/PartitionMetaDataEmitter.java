/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.process.compaction;

import java.io.IOException;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.ingestion.Synchronizer;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;
import com.google.common.collect.Lists;

/**
 * An operator used in compaction for generating partition meta data. Partition meta data defines list of
 * FileBlockMetadata which constitutes one partition file.
 */
public class PartitionMetaDataEmitter extends BaseOperator
{

  /**
   * Name of the compactionBundle. 
   * Assumption: compactionBundleName is unique name within output directory.
   * User is responsible for providing unique name within output directory.
   */
  @NotNull
  protected String compactionBundleName;

  /** Format for name of the partition files */
  protected String partFileNameFormat = "%s_%d.partition"; // compactionBundleName_partitionNo.partition

  /** Size of each partition */
  protected long partitionSizeInBytes; 

  /** File boundaries within the partition will be seperated by this symbol */
  protected String fileBoundarySeperator = "";

  /** Current partition ID */
  private long currentPartitionID;

  /** No of bytes filled in current partition */
  private long noBytesInCurrentPartition;

  /** Queue holding blocks to be written */
  private List<FileInfoBlockMetadata> blocksInProgress;

  /** Maintains blocks for the current part file */
  private List<FileInfoBlockMetadata> blocksForCurrentPartFile;

  /** Path to directory for storing intermediate blocks */
  protected transient String blocksPath;

  protected transient FileSystem appFS;

  public PartitionMetaDataEmitter()
  {
    blocksForCurrentPartFile = Lists.newArrayList();
    blocksInProgress = Lists.newLinkedList();
    currentPartitionID = 0;
    noBytesInCurrentPartition = 0;
    partitionSizeInBytes = 1024 * 1024 * 1024 * 10;// default = 10GB
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
      try {
        LOG.debug("Received {}", fileMetadata.getFileName());
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
      
      List<FileInfoBlockMetadata> blockMetaDataList = populateFileBlocksInfo(filePartitionInfo);
      blocksInProgress.addAll(blockMetaDataList);

      while (!blocksInProgress.isEmpty()) {
        long noBytesCanBePushedToCurrentPartition = partitionSizeInBytes - noBytesInCurrentPartition;
        LOG.debug("noBytesCanBePushedToCurrentPartition={}", noBytesCanBePushedToCurrentPartition);
        
        FileInfoBlockMetadata block = blocksInProgress.get(0);
        //If capacity available in the current partition is just sufficient to hold this block 
        if(block.getLength() == noBytesCanBePushedToCurrentPartition){
         //Mark this block as the last block (for current partition)
          LOG.debug("(block.getLength() is = noBytesCanBePushedToCurrentPartition");
          block = new FileInfoBlockMetadata(block.getSourceRelativePath(), block.isLastBlockSource(),
              block.getFilePath(), block.getBlockId(), block.getOffset(), 
              block.getLength(), true, -1);
          LOG.debug("block: file={} offset={} length={} blockID={}", block.getFilePath(), block.getOffset(), block.getLength(), block.getBlockId());
        }
        if (block.getLength() <= noBytesCanBePushedToCurrentPartition) {
          // Enough space available in current part file
          LOG.debug("Enough space available in current part file");
          blocksForCurrentPartFile.add(block);
          noBytesInCurrentPartition += block.getLength();
          blocksInProgress.remove(0);
          if (block.isLastBlockSource()) {
            LOG.debug("blockid {} is isLastBlockSource={}", block.getBlockId(), block.isLastBlockSource);
            // TODO:Handling Nonempty fileBoundarySeperator
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

            // Split into sub-blocks.
            //Left subblock is last block for current partition
            FileInfoBlockMetadata leftSubBlock = new FileInfoBlockMetadata(ingestionFileMetaData.getRelativePath(), block.isLastBlockSource(),
                block.getFilePath(), block.getBlockId(), block.getOffset(), noBytesCanBePushedToCurrentPartition, true, -1);
            LOG.debug("LeftSubBlock: file={} offset={} length={} blockID={}", leftSubBlock.getFilePath(), leftSubBlock.getOffset(), leftSubBlock.getLength(), leftSubBlock.getBlockId());
            //Right subblock is the block queued for subsequent partitions.
            FileInfoBlockMetadata rightSubBlock = new FileInfoBlockMetadata(ingestionFileMetaData.getRelativePath(), block.isLastBlockSource(),
                block.getFilePath(), block.getBlockId(), block.getOffset() + noBytesCanBePushedToCurrentPartition, block.getLength() - noBytesCanBePushedToCurrentPartition, false, -1);
            LOG.debug("RightSubBlock: file={} offset={} length={} blockID={}", rightSubBlock.getFilePath(), rightSubBlock.getOffset(), rightSubBlock.getLength(), rightSubBlock.getBlockId());
            blocksForCurrentPartFile.add(leftSubBlock);
            blocksInProgress.set(0, rightSubBlock);
          }
          commitPartFile();
        }
      }
    } else {
      throw new IllegalArgumentException();
    }

    

  }

  /**
   * Flush out the meta data for last partition
   */
  @Override
  public void teardown()
  {
    blocksForCurrentPartFile.addAll(blocksInProgress);
    commitPartFile();
  }
  
  

  /**
   * Populate file block information from fileMetadata.
   * This assumes that all blocks for a file are available under blocksPath
   * 
   * @param fileMetadata
   * @return
   * @throws IOException
   */
  private List<FileInfoBlockMetadata> populateFileBlocksInfo(FilePartitionInfo filePartitionInfo) throws IOException
  {
    List<FileInfoBlockMetadata> blockMetadataList = Lists.newArrayList();
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
      FileInfoBlockMetadata fileBlockMetadata = new FileInfoBlockMetadata(fileMetadata.getRelativePath(), isLastBlockSource,
          blockFilePath.toString(), blockId, 0L, length, false, previousBlockId);
      blockMetadataList.add(fileBlockMetadata);
      previousBlockId = blockId;
    }
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
    LOG.debug("Emiting patitionMetaData={}", patitionMetaData);
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
   * Block meta data with additional information about source file.
   */
  public static class FileInfoBlockMetadata extends FileBlockMetadata{
    /**
     * Relative path of the source file (w.r.t input directory)
     */
    String sourceRelativePath;
    /**
     * Is this the last block for source file
     */
    boolean isLastBlockSource;
    /**
     * Default constructor required for serialization
     */
    public FileInfoBlockMetadata()
    {
      
    }
    
    public FileInfoBlockMetadata(String sourceRelativePath, boolean isLastBlockSource,String blockFilePath, long blockId, long offset, long length, boolean isLastBlock, long previousBlockId)
    {
      super(blockFilePath, blockId, offset, length, isLastBlock, previousBlockId);
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
    
  }
  
  /**
   * Data structure used for emiting meta data about contents of partition.
   */
  public static class PatitionMetaData
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
    List<FileInfoBlockMetadata> blockMetaDataList;

    public PatitionMetaData()
    {
      
    }
    
    public PatitionMetaData(long partitionID, String partFileName, List<FileInfoBlockMetadata> blockMetaDataList)
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
    public List<FileInfoBlockMetadata> getBlockMetaDataList()
    {
      return blockMetaDataList;
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
