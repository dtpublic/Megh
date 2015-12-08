package com.datatorrent.module.io.fs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;
import com.datatorrent.lib.io.output.BlockNotFoundException;
import com.datatorrent.lib.io.output.ExtendedModuleFileMetaData;
import com.datatorrent.lib.io.output.IngestionFileMerger;

/**
 * <p>
 * HDFSFileMerger class.
 * </p>
 *
 * @since 1.0.0
 */
public class HDFSFileMerger extends IngestionFileMerger
{
  private boolean fastMergeActive;
  private long defaultBlockSize;
  private transient FastMergerDecisionMaker fastMergerDecisionMaker;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    fastMergeActive = outputFS.getConf().getBoolean("dfs.support.append", true)
        && appFS.getUri().equals(outputFS.getUri());
    LOG.debug("appFS.getUri():{}", appFS.getUri());
    LOG.debug("outputFS.getUri():{}", outputFS.getUri());
    defaultBlockSize = outputFS.getDefaultBlockSize(new Path(filePath));
    fastMergerDecisionMaker = new FastMergerDecisionMaker(blocksDir, appFS, defaultBlockSize);
  }

  @Override
  protected void mergeBlocks(ExtendedModuleFileMetaData fileMetadata) throws IOException
  {

    try {
      LOG.debug("fastMergeActive: {}", fastMergeActive);
      if (fastMergeActive && fastMergerDecisionMaker.isFastMergePossible(fileMetadata)
          && fileMetadata.getNumberOfBlocks() > 0) {
        LOG.debug("Using fast merge on HDFS.");
        concatBlocks(fileMetadata);
        return;
      }
      LOG.debug("Falling back to slow merge on HDFS.");
      super.mergeBlocks(fileMetadata);

    } catch (BlockNotFoundException e) {
      if (recover(fileMetadata)) {
        LOG.debug("Recovery attempt successful.");
        successfulFiles.add(fileMetadata);
      } else {
        failedFiles.add(fileMetadata);
      }
    }
  }

  private void concatBlocks(ModuleFileMetaData fileMetadata) throws IOException
  {
    Path outputFilePath = new Path(filePath, fileMetadata.getRelativePath());

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksDir, Long.toString(blocksArray[0]));
    if (numBlocks > 1) {
      Path[] blockFiles = new Path[numBlocks - 1]; // Leave the first block

      for (int index = 1; index < numBlocks; index++) {
        blockFiles[index - 1] = new Path(blocksDir, Long.toString(blocksArray[index]));
      }

      outputFS.concat(firstBlock, blockFiles);
    }

    moveToFinalFile(firstBlock, outputFilePath);
  }

  @VisibleForTesting
  protected boolean recover(ModuleFileMetaData iFileMetadata) throws IOException
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

  public static class FastMergerDecisionMaker
  {

    private String blocksDir;
    private FileSystem appFS;
    private long defaultBlockSize;

    public FastMergerDecisionMaker(String blocksDir, FileSystem appFS, long defaultBlockSize)
    {
      this.blocksDir = blocksDir;
      this.appFS = appFS;
      this.defaultBlockSize = defaultBlockSize;
    }

    public boolean isFastMergePossible(ModuleFileMetaData fileMetadata) throws IOException, BlockNotFoundException
    {
      short replicationFactor = 0;
      boolean sameReplicationFactor = true;
      boolean multipleOfBlockSize = true;

      int numBlocks = fileMetadata.getNumberOfBlocks();
      LOG.debug("fileMetadata.getNumberOfBlocks(): {}", fileMetadata.getNumberOfBlocks());
      long[] blocksArray = fileMetadata.getBlockIds();
      LOG.debug("fileMetadata.getBlockIds().len: {}", fileMetadata.getBlockIds().length);

      for (int index = 0; index < numBlocks && (sameReplicationFactor && multipleOfBlockSize); index++) {
        Path blockFilePath = new Path(blocksDir + Path.SEPARATOR + blocksArray[index]);
        if (!appFS.exists(blockFilePath)) {
          throw new BlockNotFoundException(blockFilePath);
        }
        FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[index]));
        if (index == 0) {
          replicationFactor = status.getReplication();
          LOG.debug("replicationFactor: {}", replicationFactor);
        } else {
          sameReplicationFactor = (replicationFactor == status.getReplication());
          LOG.debug("sameReplicationFactor: {}", sameReplicationFactor);
        }

        if (index != numBlocks - 1) {
          multipleOfBlockSize = (status.getLen() % defaultBlockSize == 0);
          LOG.debug("multipleOfBlockSize: {}", multipleOfBlockSize);
        }
      }
      return sameReplicationFactor && multipleOfBlockSize;
    }
  }

}
