/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.bandwidth.BandwidthLimitingOperator;
import com.datatorrent.lib.bandwidth.BandwidthManager;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.block.ModuleBlockMetadata;
import com.datatorrent.lib.io.output.OutputFileMetaData;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputFileBlockMetaData;

/**
 * ModuleFileSplitter extends {@link FileSplitterInput} to add following
 * features:<br/>
 * 1. Bandwidth Limitation on input rate<br/>
 * 2. Option to do sequential or parallel read of input file<br/>
 * 3. Exposes terminateApp flag which can be set by downstream operator through
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{@link StatsListener}<br/>
 * 4. Modified scanner to ignore Symbolic links, copy blank directory from
 * input, scan files with similar names residing in different input directories.
 */
public class ModuleFileSplitter extends FileSplitterInput implements BandwidthLimitingOperator
{
  private static Logger LOG = LoggerFactory.getLogger(ModuleFileSplitter.class);
  private transient FileSystem appFS;
  private BandwidthManager bandwidthManager;
  private FileBlockMetadata currentBlockMetadata;
  private boolean sequencialFileRead;
  //termiateApp: For one time copy, if set to true using stats listener, ShutdownException will be thrown.
  private boolean termiateApp = false;

  public ModuleFileSplitter()
  {
    super();
    super.setScanner(new Scanner());
    bandwidthManager = new BandwidthManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    bandwidthManager.setup(context);
  }

  @Override
  protected long getDefaultBlockSize()
  {
    try {
      if (appFS == null) {
        appFS = getLocalFS();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to get FileSystem instance.", e);
    }
    //getting block size of local HDFS by default, to optimize the blocks for fast merge
    return appFS.getDefaultBlockSize(new Path(context.getValue(DAGContext.APPLICATION_PATH)));
  }

  protected FileSystem getLocalFS() throws IOException
  {
    return FileSystem.newInstance(new Configuration());
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (((Scanner)getScanner()).isOneTimeCopy() && ((Scanner)getScanner()).isFirstScanComplete()
        && blockMetadataIterator == null) {
      try {
        checkCompletion();
      } catch (IOException e) {
        throw new RuntimeException("Unable to check shutdown signal file.", e);
      }
    }
  }

  private void checkCompletion() throws IOException
  {
    if (termiateApp) {
      LOG.info("Application completed. Sending shutdown signal.");
      throw new ShutdownException();
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bandwidthManager.teardown();
    if (appFS != null) {
      try {
        appFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close application file system.", e);
      }
    }
  }

  @Override
  protected void process()
  {
    if (blockMetadataIterator != null) {
      if (!emitBlockMetadata()) {
        return;
      }
    }

    FileInfo fileInfo;
    while ((fileInfo = getFileInfo()) != null) {
      if (!processFileInfo(fileInfo)) {
        break;
      }
    }
  }

  @Override
  protected boolean emitBlockMetadata()
  {
    boolean emitNext = true;
    while (emitNext) {
      if (currentBlockMetadata == null) {
        if (blockMetadataIterator.hasNext()) {
          currentBlockMetadata = blockMetadataIterator.next();
        } else {
          blockMetadataIterator = null;
          LOG.info("Done processing file.");
          break;
        }
      }
      emitNext = emitCurrentBlockMetadata();
    }
    return emitNext;
  }

  private boolean emitCurrentBlockMetadata()
  {
    long currBlockSize = currentBlockMetadata.getLength() - currentBlockMetadata.getOffset();
    if ((!bandwidthManager.isBandwidthRestricted() && blockCount < blocksThreshold)
        || (bandwidthManager.isBandwidthRestricted() && bandwidthManager.canConsumeBandwidth())) {
      this.blocksMetadataOutput.emit(currentBlockMetadata);
      super.blockCount++;
      currentBlockMetadata = null;
      bandwidthManager.consumeBandwidth(currBlockSize);
      return true;
    }
    return false;
  }

  public BandwidthManager getBandwidthManager()
  {
    return bandwidthManager;
  }

  public void setBandwidthManager(BandwidthManager bandwidthManager)
  {
    this.bandwidthManager = bandwidthManager;
  }

  @Override
  protected void updateReferenceTimes(ScannedFileInfo fileInfo)
  {
    Map<String, Long> lastModifiedTimeMap = ((Scanner)getScanner()).getLastModifiedTimeMap(fileInfo.getFilePath());
    lastModifiedTimeMap.put(fileInfo.getDirectoryPath(), fileInfo.getModifiedTime());
  }

  public boolean isSequencialFileRead()
  {
    return sequencialFileRead;
  }

  public void setSequencialFileRead(boolean sequencialFileRead)
  {
    this.sequencialFileRead = sequencialFileRead;
  }

  public boolean isTermiateApp()
  {
    return termiateApp;
  }

  public void setTermiateApp(boolean termiateApp)
  {
    this.termiateApp = termiateApp;
  }

  public static class Scanner extends TimeBasedDirectoryScanner
  {
    private String ignoreFilePatternRegularExp;
    private transient Pattern ignoreRegex;
    private boolean oneTimeCopy;
    private boolean firstScanComplete;

    @NotNull
    protected final Map<String, Map<String, Long>> inputDirTolastModifiedTimes;

    public Scanner()
    {
      super();
      inputDirTolastModifiedTimes = Maps.newConcurrentMap();
    }

    @Override
    public void setup(OperatorContext context)
    {
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }
      super.setup(context);
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (!accepted) {
        return false;
      }
      String fileName = new Path(filePathStr).getName();
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(fileName);
        // If matched against ignored Regex then do not accept the file.
        if (matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected void scan(Path filePath, Path rootPath)
    {
      Map<String, Long> lastModifiedTimesForInputDir;
      lastModifiedTimesForInputDir = getLastModifiedTimeMap(filePath.toUri().getPath());
      scan(filePath, rootPath, lastModifiedTimesForInputDir);
    }

    private void scan(Path filePath, Path rootPath, Map<String, Long> lastModifiedTimesForInputDir)
    {
      try {
        FileStatus parentStatus = fs.getFileStatus(filePath);
        String parentPathStr = filePath.toUri().getPath();

        LOG.debug("scan {}", parentPathStr);

        FileStatus[] childStatuses = fs.listStatus(filePath);

        if (childStatuses.length == 0 && rootPath == null && lastModifiedTimesForInputDir.get(parentPathStr) == null) { // empty input directory copy as is
          ScannedFileInfo info = new ScannedFileInfo(null, filePath.toString(), parentStatus.getModificationTime());
          processDiscoveredFile(info);
          lastModifiedTimesForInputDir.put(parentPathStr, parentStatus.getModificationTime());
        }

        for (FileStatus childStatus : childStatuses) {
          Path childPath = childStatus.getPath();
          String childPathStr = childPath.toUri().getPath();

          if (childStatus.isSymlink()) {
            ignoredFiles.add(childPathStr);
          } else if (childStatus.isDirectory() && isRecursive()) {
            addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
            scan(childPath, rootPath == null ? parentStatus.getPath() : rootPath, lastModifiedTimesForInputDir);
          } else if (acceptFile(childPathStr)) {
            addToDiscoveredFiles(rootPath, parentStatus, childStatus, lastModifiedTimesForInputDir);
          } else {
            // don't look at it again
            ignoredFiles.add(childPathStr);
          }
        }
      } catch (FileNotFoundException fnf) {
        LOG.warn("Failed to list directory {}", filePath, fnf);
      } catch (IOException e) {
        throw new RuntimeException("listing files", e);
      }
    }

    private void addToDiscoveredFiles(Path rootPath, FileStatus parentStatus, FileStatus childStatus,
        Map<String, Long> lastModifiedTimesForInputDir) throws IOException
    {
      Path childPath = childStatus.getPath();
      String childPathStr = childPath.toUri().getPath();
      // Directory by now is scanned forcibly. Now check for whether file/directory needs to be added to discoveredFiles.
      Long oldModificationTime = lastModifiedTimesForInputDir.get(childPathStr);
      lastModifiedTimesForInputDir.put(childPathStr, childStatus.getModificationTime());

      if (skipFile(childPath, childStatus.getModificationTime(), oldModificationTime) || // Skip dir or file if no timestamp modification
          (childStatus.isDirectory() && (oldModificationTime != null))) { // If timestamp modified but if its a directory and already present in map, then skip.
        return;
      }

      if (ignoredFiles.contains(childPathStr)) {
        return;
      }

      ScannedFileInfo info = createScannedFileInfo(parentStatus.getPath(), parentStatus, childPath, childStatus,
          rootPath);

      LOG.debug("Processing file: " + info.getFilePath());
      processDiscoveredFile(info);
    }

    private Map<String, Long> getLastModifiedTimeMap(String key)
    {
      if (inputDirTolastModifiedTimes.get(key) == null) {
        Map<String, Long> modifiedTimeMap = Maps.newHashMap();
        inputDirTolastModifiedTimes.put(key, modifiedTimeMap);
      }
      return inputDirTolastModifiedTimes.get(key);
    }

    @Override
    protected void scanIterationComplete()
    {
      super.scanIterationComplete();
      if (oneTimeCopy) {
        super.stopScanning();
      }
      firstScanComplete = true;
    }

    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegularExp)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegularExp;
      this.ignoreRegex = Pattern.compile(ignoreFilePatternRegularExp);
    }

    public boolean isOneTimeCopy()
    {
      return oneTimeCopy;
    }

    public void setOneTimeCopy(boolean oneTimeCopy)
    {
      this.oneTimeCopy = oneTimeCopy;
    }

    public boolean isFirstScanComplete()
    {
      return firstScanComplete;
    }

    public void setFirstScanComplete(boolean firstScanComplete)
    {
      this.firstScanComplete = firstScanComplete;
    }
  }

  @Override
  protected FileMetadata createFileMetadata(FileInfo fileInfo)
  {
    return new ModuleFileMetaData(fileInfo.getFilePath());
  }

  @Override
  protected ModuleFileMetaData buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    FileMetadata metadata = super.buildFileMetadata(fileInfo);
    ModuleFileMetaData moduleFileMetaData = (ModuleFileMetaData)metadata;

    Path path = new Path(fileInfo.getFilePath());
    FileStatus status = getFileStatus(path);
    if (fileInfo.getDirectoryPath() == null) { // Direct filename is given as input.
      moduleFileMetaData.setRelativePath(status.getPath().getName());
    } else {
      String relativePath = getRelativePathWithFolderName(fileInfo);
      moduleFileMetaData.setRelativePath(relativePath);
    }
    LOG.debug("****FileMetadata: "+moduleFileMetaData.toString());
    moduleFileMetaData.setOutputBlockMetaDataList(populateOutputFileBlockMetaData(moduleFileMetaData));
    return moduleFileMetaData;
  }


  public List<OutputBlock> populateOutputFileBlockMetaData(ModuleFileMetaData fileMetadata){
    List<OutputBlock> outputBlockMetaDataList = Lists.newArrayList();
    if(!fileMetadata.isDirectory()){
      Iterator<FileBlockMetadata> fileBlockMetadataIterator = new BlockMetadataIterator(this, fileMetadata, blockSize);
      while(fileBlockMetadataIterator.hasNext()){
        FileBlockMetadata fmd = fileBlockMetadataIterator.next();
        OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(fmd, fileMetadata.relativePath, fileBlockMetadataIterator.hasNext());
        outputBlockMetaDataList.add(outputFileBlockMetaData);
      }
    }
    return outputBlockMetaDataList;
  }
  @Override
  protected ModuleBlockMetadata createBlockMetadata(FileMetadata fileMetadata)
  {
    ModuleBlockMetadata blockMetadta = new ModuleBlockMetadata(fileMetadata.getFilePath());
    blockMetadta.setReadBlockInSequence(sequencialFileRead);
    return blockMetadta;
  }

  @Override
  protected ModuleBlockMetadata buildBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber,
      FileMetadata fileMetadata, boolean isLast)
  {
    FileBlockMetadata metadata = super.buildBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
    ModuleBlockMetadata blockMetadata = (ModuleBlockMetadata)metadata;
    return blockMetadata;
  }

  /*
   * As folder name was given to input for copy, prefix folder name to the sub items to copy.
   */
  private String getRelativePathWithFolderName(FileInfo fileInfo)
  {
    String parentDir = new Path(fileInfo.getDirectoryPath()).getName();
    return parentDir + File.separator + fileInfo.getRelativeFilePath();
  }

  public static class ModuleFileMetaData extends ModuleFileSplitter.FileMetadata implements OutputFileMetaData
  {
    private String relativePath;
    private List<OutputBlock> outputBlockMetaDataList;

    protected ModuleFileMetaData()
    {
      super();
      outputBlockMetaDataList = Lists.newArrayList();
    }

    public ModuleFileMetaData(@NotNull String filePath)
    {
      super(filePath);
    }

    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    public String getOutputRelativePath()
    {
      return relativePath;
    }
    
    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData#getOutputBlocksList()
     */
    @Override
    public List<OutputBlock> getOutputBlocksList()
    {
      return outputBlockMetaDataList;
    }
    
    /**
     * @param outputBlockMetaDataList the outputBlockMetaDataList to set
     */
    public void setOutputBlockMetaDataList(List<OutputBlock> outputBlockMetaDataList)
    {
      this.outputBlockMetaDataList = outputBlockMetaDataList;
    }

    @Override
    public String toString()
    {
      return "ModuleFileMetaData [relativePath=" + relativePath + ", getNumberOfBlocks()=" + getNumberOfBlocks()
          + ", getFileName()=" + getFileName() + ", getFileLength()=" + getFileLength() + ", isDirectory()="
          + isDirectory() + "]";
    }

  }
}
