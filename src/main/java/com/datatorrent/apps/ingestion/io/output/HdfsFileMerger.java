package com.datatorrent.apps.ingestion.io.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;

public class HdfsFileMerger extends FileMerger
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
    if (fastMergeActive && fastMergerPossible(fileMetadata)) {
      LOG.debug("Using fast merge on HDFS.");
      stitchAndAppend(fileMetadata);
      return;
    }
    LOG.debug("Falling back to slow merge on HDFS.");
    super.mergeBlocks(fileMetadata);

  }

  private boolean fastMergerPossible(IngestionFileMetaData fileMetadata) throws IOException
  {
    short replicationFactor = 0;
    boolean sameReplication = true;
    boolean multipleOfBlockSize = true;

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    if (numBlocks > 0) {
      FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[0]));
      replicationFactor = status.getReplication();
      multipleOfBlockSize = status.getLen() % defaultBlockSize == 0;
    }
    for (int index = 1; index < numBlocks && sameReplication && multipleOfBlockSize; index++) {
      FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[index]));
      sameReplication = replicationFactor == status.getReplication();
      if (index != numBlocks) {
        multipleOfBlockSize = status.getLen() % defaultBlockSize == 0;
      }
    }
    return sameReplication && multipleOfBlockSize;
  }

  private void stitchAndAppend(IngestionFileMetaData fileMetadata) throws IOException
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
    moveFile(firstBlock, outputFilePath);
  }

  /*
   * @VisibleForTesting protected boolean recover(IngestionFileMetaData iFileMetadata) { try { Path firstBlockPath = new
   * Path(blocksDir + Path.SEPARATOR + iFileMetadata.getBlockIds()[0]); String absolutePath = filePath + Path.SEPARATOR
   * + iFileMetadata.getRelativePath(); Path outputFilePath = new Path(absolutePath); if (appFS.exists(firstBlockPath))
   * { FileStatus status = appFS.getFileStatus(firstBlockPath); if (status.getLen() == iFileMetadata.getFileLength()) {
   * moveFile(firstBlockPath, outputFilePath); return true; } else {
   * LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath); return false; } } else { if
   * (outputFS.exists(outputFilePath)) {
   * LOG.debug("Output file already present at the destination, nothing to recover."); return true; }
   * LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath); return false; } } catch (IOException e)
   * { LOG.error("Error in recovering.", e); throw new RuntimeException("Unable to recover."); } }
   */
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileMerger.class);

}
