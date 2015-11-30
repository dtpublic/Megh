/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.monitor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.NotNull;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

public class KeyTracker extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(KeyTracker.class);

  public enum FinalizationPolicy
  {
    TIMEBASED,
    CATEGORICAL
  }

  private FinalizationPolicy finalizationPolicy = FinalizationPolicy.TIMEBASED;
  private long activeAggregationInterval = 0;
  private int maxAggregateStored = 0;

  @NotNull
  private String configurationSchemaJSON;
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  @VisibleForTesting
  protected transient DimensionalConfigurationSchema configurationSchema;

  private Map<Long, Set<FinalizedKey>> timeBucketKeys;
  private Queue<FinalizedKey> nonTimeBasedKeys;
  private long largestTimeBucketEmitted = 0;

  public transient DefaultOutputPort<FinalizedKey> finalizedKey = new DefaultOutputPort<>();

  public transient DefaultOutputPort<Aggregate> partialAggregates = new DefaultOutputPort<>();

  public transient DefaultInputPort<Aggregate> input = new DefaultInputPort<Aggregate>()
  {
    @Override
    public void process(Aggregate tuple)
    {
      partialAggregates.emit(tuple);
      processInputTuple(tuple);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    if (finalizationPolicy == FinalizationPolicy.TIMEBASED) {
      timeBucketKeys = Maps.newLinkedHashMap();
    } else if (finalizationPolicy == FinalizationPolicy.CATEGORICAL) {
      nonTimeBasedKeys = Lists.newLinkedList();
    } else {
      throw new RuntimeException("Application should be configured for at least one of the time based and categorical based aggregations.");
    }

    aggregatorRegistry.setup();

    if (configurationSchema == null) {
      configurationSchema = new DimensionalConfigurationSchema(configurationSchemaJSON, aggregatorRegistry);
    }
  }

  protected void processInputTuple(Aggregate tuple)
  {
    GPOMutable key = tuple.getKeys();
    FieldsDescriptor keyDescriptor = this.configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(tuple.getDimensionDescriptorID());
    key.setFieldDescriptor(keyDescriptor);

    FinalizedKey data = new FinalizedKey(tuple.getDimensionDescriptorID(), key);

    if (finalizationPolicy == FinalizationPolicy.TIMEBASED) {
      storeTimeBasedKey(data);
    } else if (finalizationPolicy == FinalizationPolicy.CATEGORICAL) {
      storeNonTimeBasedKey(data);
    } else {
      throw new RuntimeException("Unknown finalization policy set.");
    }
  }

  private void storeTimeBasedKey(FinalizedKey data)
  {
    GPOMutable key = data.getKey();
    if (!key.getFieldDescriptor().getFieldToType().containsKey(DimensionsDescriptor.DIMENSION_TIME)) {
      throw new RuntimeException("Expected time based data, but did not receive time as key");
    }

    Long bucketTime = findTimeBucket(key);

    // Exclude everything before OR at largest time bucket emitted.
    if (bucketTime.longValue() <= largestTimeBucketEmitted) {
      return;
    }

    Set<FinalizedKey> storedData;
    if (!timeBucketKeys.containsKey(bucketTime)) {
      storedData = Sets.newLinkedHashSet();
      timeBucketKeys.put(bucketTime, storedData);
    } else {
      storedData = timeBucketKeys.get(bucketTime);
    }

    storedData.add(data);
  }

  private Long findTimeBucket(GPOMutable key)
  {
    Long newTimeBucket;

    int bucketOrdinal = key.getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET);
    TimeBucket bucket = TimeBucket.values()[bucketOrdinal];

    newTimeBucket = bucket.roundDown(key.getFieldLong(DimensionsDescriptor.DIMENSION_TIME)) + bucket.getTimeUnit().toMillis(1);

    return newTimeBucket;
  }

  private void storeNonTimeBasedKey(FinalizedKey data)
  {
    if (!nonTimeBasedKeys.contains(data)) {
      nonTimeBasedKeys.offer(data);
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    if (finalizationPolicy == FinalizationPolicy.TIMEBASED) {
      handleTimeBasedFinalizedData();
    } else if (finalizationPolicy == FinalizationPolicy.CATEGORICAL) {
      handleNonTimeBasedFinalizedData();
    } else {
      throw new RuntimeException("Unknown finalization policy set.");
    }
  }

  private void handleTimeBasedFinalizedData()
  {
    // Emit all the buckets which are before SystemTime - aggInterval
    long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis());
    long emitBeforeTime = currentTime - activeAggregationInterval;

    Iterator<Entry<Long, Set<FinalizedKey>>> entries = timeBucketKeys.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<Long, Set<FinalizedKey>> entry = entries.next();
      Long bucketTime = entry.getKey();

      if (bucketTime.longValue() > emitBeforeTime) {
        continue;
      }

      for (FinalizedKey data : entry.getValue()) {
        finalizedKey.emit(data);
      }

      if (largestTimeBucketEmitted < bucketTime.longValue()) {
        largestTimeBucketEmitted = bucketTime;
      }

      entries.remove();
    }
  }

  private void handleNonTimeBasedFinalizedData()
  {
    int currentQueueSize = nonTimeBasedKeys.size();
    while (currentQueueSize > maxAggregateStored) {
      finalizedKey.emit(nonTimeBasedKeys.poll());
      --currentQueueSize;
    }
  }

  public String getConfigurationSchemaJSON()
  {
    return configurationSchemaJSON;
  }

  public void setConfigurationSchemaJSON(String configurationSchemaJSON)
  {
    this.configurationSchemaJSON = configurationSchemaJSON;
  }

  public long getActiveAggregationInterval()
  {
    return activeAggregationInterval;
  }

  public void setActiveAggregationInterval(long activeAggregationInterval)
  {
    this.activeAggregationInterval = TimeBucket.MINUTE.roundDown(activeAggregationInterval * 1000);
  }

  public int getMaxAggregateStored()
  {
    return maxAggregateStored;
  }

  public void setMaxAggregateStored(int maxAggregateStored)
  {
    this.maxAggregateStored = maxAggregateStored;
  }

  public FinalizationPolicy getFinalizationPolicy()
  {
    return finalizationPolicy;
  }

  public void setFinalizationPolicy(FinalizationPolicy finalizationPolicy)
  {
    this.finalizationPolicy = finalizationPolicy;
  }

  public static class FinalizedKey implements Serializable
  {
    private static final long serialVersionUID = 20150720112000L;

    private int ddID;
    private GPOMutable key;

    public FinalizedKey()
    {
      /* Empty constructor for kryo */
    }

    public FinalizedKey(int ddID, GPOMutable key)
    {
      this.ddID = ddID;
      this.key = key;
    }

    public int getDdID()
    {
      return ddID;
    }

    public void setDdID(int ddID)
    {
      this.ddID = ddID;
    }

    public GPOMutable getKey()
    {
      return key;
    }

    public void setKey(GPOMutable key)
    {
      this.key = key;
    }

    @Override
    public int hashCode()
    {
      return GPOUtils.hashcode(this.getKey());
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof FinalizedKey)) {
        return false;
      }
      final FinalizedKey other = (FinalizedKey)obj;

      if (this.getKey().equals(other.getKey())) {
        return true;
      }

      return false;
    }
  }
}
