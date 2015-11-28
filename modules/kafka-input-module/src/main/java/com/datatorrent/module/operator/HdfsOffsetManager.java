/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.contrib.kafka.KafkaPartition;
import com.datatorrent.contrib.kafka.OffsetManager;
/**
 * HdfsOffsetManager is a plugin for Kafka Input Operator and
 * serves the functionality of OffsetManager. &nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: No Output Port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName HdfsOffsetManager
 * @category offset Manager
 *
 */

public class HdfsOffsetManager implements OffsetManager
{
  private static final Logger LOG = LoggerFactory.getLogger(HdfsOffsetManager.class);

  @NotNull
  private String offsetFilePath;

  @NotNull
  private String delimiter;

  @NotNull
  private String topicName;

  /**
   * Load the offsets from the offsetFilePath and the operator consume messages from these offsets.
   * @return the offsetMap
   */
  @Override
  public Map<KafkaPartition, Long> loadInitialOffsets()
  {
    Map<KafkaPartition, Long> offsetMap = new HashMap<KafkaPartition, Long>();

    Configuration configuration = new Configuration();
    Path dataPath = new Path(offsetFilePath);

    InputStreamReader stream = null;
    BufferedReader br = null;

    try (FileSystem fs = getFileSystem(configuration, dataPath)) {
      if (fs.exists(dataPath) && fs.isFile(dataPath)) {
        LOG.info("reading offset file " + dataPath);
        stream = new InputStreamReader(fs.open(dataPath));
        br = new BufferedReader(stream);
        String line;
        line = br.readLine();
        while (line != null) {
          StringTokenizer st = new StringTokenizer(line, delimiter);
          int count = st.countTokens();
          while (st.hasMoreTokens()) {
            if (count == 2) {
              offsetMap.put(new KafkaPartition(topicName, Integer.parseInt(st.nextToken())),
                  Long.parseLong(st.nextToken()));
            } else if (count == 3) {
              offsetMap.put(new KafkaPartition(st.nextToken(), topicName, Integer.parseInt(st.nextToken())),
                  Long.parseLong(st.nextToken()));
            }
          }
          line = br.readLine();
        }
      }
      return offsetMap;
    } catch (IOException e) {
      throw new RuntimeException("Error reading offset file [ " + offsetFilePath + " ]" + e);
    } finally {
      closeStreamReader(stream);
      closeStreamReader(br);
    }
  }

  private void closeStreamReader(Reader reader)
  {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while stream closing " + e);
    }
  }

  private FileSystem getFileSystem(final Configuration configuration, final Path dataPath)
    throws IOException
  {
    return FileSystem.get(dataPath.toUri(), configuration);
  }

  @Override
  public void updateOffsets(Map<KafkaPartition, Long> kafkaPartitionLongMap)
  {
    return;
  }

  /**
   * Return the offsetFilePath
   * @return
   */
  public String getOffsetFilePath()
  {
    return offsetFilePath;
  }

  /**
   * Set the offsetFilePath with the given file path
   * @param offsetFilePath
   */
  public void setOffsetFilePath(@NotNull String offsetFilePath)
  {
    this.offsetFilePath = offsetFilePath;
  }

  /**
   * Return the data delimiter
   * @return
   */
  public String getDelimiter()
  {
    return delimiter;
  }

  /**
   * Set the delimiter while reading the offset info from file
   * @param delimiter
   */
  public void setDelimiter(@NotNull String delimiter)
  {
    this.delimiter = delimiter;
  }

  /**
   * Return the topic name
   * @return
   */
  public String getTopicName()
  {
    return topicName;
  }

  /**
   * Set the topic name from where these offsets relates to
   * @param topicName
   */
  public void setTopicName(@NotNull String topicName)
  {
    this.topicName = topicName;
  }
}
