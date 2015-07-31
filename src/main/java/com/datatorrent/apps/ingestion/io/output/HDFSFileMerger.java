package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.common.BlockNotFoundException;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.google.common.annotations.VisibleForTesting;


/**
 * <p>HDFSFileMerger class.</p>
 *
 * @since 1.0.0
 */
public class HDFSFileMerger extends IngestionFileMerger
{
  private boolean fastMergeActive;
  private long defaultBlockSize;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    fastMergeActive = outputFS.getConf().getBoolean("dfs.support.append", true) && appFS.getUri().equals(outputFS.getUri());
    defaultBlockSize = outputFS.getDefaultBlockSize(new Path(filePath));
  }

  @Override
  protected void mergeBlocks(IngestionFileMetaData fileMetadata) throws IOException
  {
    
    try {
      if (fastMergeActive && fastMergerPossible(fileMetadata) && fileMetadata.getNumberOfBlocks() > 0) {
        LOG.debug("Using fast merge on HDFS.");
        concatBlocks(fileMetadata);
        return;
      }
      LOG.debug("Falling back to slow merge on HDFS.");
      super.mergeBlocks(fileMetadata);
      
    } catch (BlockNotFoundException e) {
      if(recover(fileMetadata)){
        LOG.debug("Recovery attempt successful.");
        successfulFiles.add(fileMetadata);
      }else{
        failedFiles.add(fileMetadata);
      }
    }
  }

  private boolean fastMergerPossible(IngestionFileMetaData fileMetadata) throws IOException, BlockNotFoundException
  {
    short replicationFactor = 0;
    boolean sameReplicationFactor = true;
    boolean multipleOfBlockSize = true;

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    for (int index = 0; index < numBlocks && (sameReplicationFactor && multipleOfBlockSize); index++) {
      Path blockFilePath = new Path(blocksDir + Path.SEPARATOR + blocksArray[index]);
      if(! appFS.exists(blockFilePath)){
        throw new BlockNotFoundException(blockFilePath);
      }
      FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[index]));
      if (index == 0) {
        replicationFactor = status.getReplication();
      } else {
        sameReplicationFactor = (replicationFactor == status.getReplication());
      }

      if (index != numBlocks) {
        multipleOfBlockSize = (status.getLen() % defaultBlockSize == 0);
      }
    }
    return sameReplicationFactor && multipleOfBlockSize;
  }

  private void concatBlocks(IngestionFileMetaData fileMetadata) throws IOException
  {
    Path outputFilePath = new Path(filePath, fileMetadata.getRelativePath());

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksDir, Long.toString(blocksArray[0]));
    Path[] blockFiles = new Path[numBlocks - 1]; // Leave the first block

    for (int index = 1; index < numBlocks; index++) {
      blockFiles[index - 1] = new Path(blocksDir, Long.toString(blocksArray[index]));
    }

    outputFS.concat(firstBlock, blockFiles);
    moveToFinalFile(firstBlock, outputFilePath);
  }

  @VisibleForTesting
  protected boolean recover(IngestionFileMetaData iFileMetadata) throws IOException
  {
    Path firstBlockPath = new Path(blocksDir + Path.SEPARATOR + iFileMetadata.getBlockIds()[0]);
    Path outputFilePath = new Path(filePath, iFileMetadata.getRelativePath());
    if (appFS.exists(firstBlockPath)) {
      FileStatus status = appFS.getFileStatus(firstBlockPath);
      if (status.getLen() == iFileMetadata.getFileLength()) {
        moveToFinalFile(firstBlockPath, outputFilePath);
        return true;
      }
      LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
      return false;
    }

    if (outputFS.exists(outputFilePath)) {
      LOG.debug("Output file already present at the destination, nothing to recover.");
      return true;
    }
    LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
    return false;
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileMerger.class);

}
