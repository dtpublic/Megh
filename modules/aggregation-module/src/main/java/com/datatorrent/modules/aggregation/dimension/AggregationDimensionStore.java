/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.dimension;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.datatorrent.modules.aggregation.monitor.KeyTracker;
import com.datatorrent.netlet.util.Slice;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class AggregationDimensionStore extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final Logger logger = LoggerFactory.getLogger(AggregationDimensionStore.class);

  private static final long serialVersionUID = 1435565168L;

  private transient Map<Integer, String> aggregatorIDToNameMap;

  private transient boolean deleteFinalizedData = false;

  public static final int staticOTFAggID = Integer.MAX_VALUE;

  private int operatorId = -1;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> finalizedData = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    if (operatorId == -1) {
      operatorId = context.getId();
    }
    long currentBucketId = getBucketID();
    setBucketID(operatorId * 1000000 + currentBucketId);
    super.setup(context);
  }

  private void emitFinalizedData(int ddID, GPOMutable key)
  {
    Map<String, Aggregate> aggregatesOfKey = Maps.newLinkedHashMap();

    IntArrayList aggIDList = configurationSchema.getDimensionsDescriptorIDToAggregatorIDs().get(ddID);

    for (int aggIDIndex = 0; aggIDIndex < aggIDList.size(); aggIDIndex++) {
      int aggID = aggIDList.get(aggIDIndex);

      // Generate EventKey using dimension descriptor, aggregator and above created GPOMutable object.
      // This event key will be used to query operator cache first. If not found there, it queries HDHT cache.
      // If is not found in HDHT cache, it does lookup into filesystem. (Lookup to HDHT cache and filesystem both is
      // done by "load" method)
      EventKey evKey = new EventKey(getSchemaID(), ddID, aggID, key);
      Aggregate aggregate = cache.get(evKey);
      if (aggregate == null) {
        aggregate = load(evKey);
      }

      // If partialAggregates are present add to list.
      if (aggregate == null) {
        // This is a possible case when the timebucket is 1h and the application just started, there is no data
        // generated for 1hr aggregate window.
        logger.info("Aggregate not found for EventKey: {}. Skipping this complete ddID", evKey);
        return;
      } else {
        aggregatesOfKey.put(getAggregatorNameFromId(aggregate.getAggregatorID()), aggregate);
        if (deleteFinalizedData) {
          deleteFromHDHT(evKey);
        }
      }
    }

    populateOTFAggregator(aggregatesOfKey, ddID, key);

    if (aggregatesOfKey.size() != 0) {
      try {
        generateAndEmit(key, aggregatesOfKey, ddID);
      } catch (JSONException e) {
        logger.error("Failed to generate JSON finalized data. SKipping this time...", e);
      }
    }
  }

  private void generateAndEmit(GPOMutable key, Map<String, Aggregate> aggregatesOfKey, int ddID) throws JSONException
  {
    JSONObject toBeEmitted = new JSONObject();

    toBeEmitted.put("groupByKey", generateKey(key));
    toBeEmitted.put("aggregates", generateAggregates(aggregatesOfKey, ddID));

    finalizedData.emit(toBeEmitted.toString());
  }

  private JSONObject generateAggregates(Map<String, Aggregate> aggregatesOfKey, int ddID) throws JSONException
  {
    Map<String, Set<String>> valueToOTFAggregator = configurationSchema.getDimensionsDescriptorIDToValueToOTFAggregator().get(ddID);
    Map<String, Set<String>> valueToAggregator = configurationSchema.getDimensionsDescriptorIDToValueToAggregator().get(ddID);

    JSONObject jsonAggregates = new JSONObject();

    for (Map.Entry<String, Aggregate> entry : aggregatesOfKey.entrySet()) {
      String aggFunc = entry.getKey();
      GPOMutable aggregates = entry.getValue().getAggregates();
      List<String> fieldList = aggregates.getFieldDescriptor().getFieldList();

      for (String field : fieldList) {
        JSONObject val;
        if (jsonAggregates.has(field)) {
          val = (JSONObject)jsonAggregates.get(field);
        } else {
          val = new JSONObject();
          jsonAggregates.put(field, val);
        }

        if (valueToAggregator.containsKey(field) && valueToAggregator.get(field).contains(aggFunc)) {
          val.put(aggFunc, aggregates.getField(field));
        } else if (valueToOTFAggregator.containsKey(field) && valueToOTFAggregator.get(field).contains(aggFunc)) {
          val.put(aggFunc, aggregates.getField(field));
        } else {
          logger.debug("Agg: {} not set for field: {}", aggFunc, field);
        }
      }
    }

    return jsonAggregates;
  }

  private JSONObject generateKey(GPOMutable key) throws JSONException
  {
    JSONObject jsonKey = new JSONObject();
    List<String> fieldList = key.getFieldDescriptor().getFieldList();
    if (fieldList.contains(DimensionsDescriptor.DIMENSION_TIME)) {
      jsonKey.put(DimensionsDescriptor.DIMENSION_TIME, key.getField(DimensionsDescriptor.DIMENSION_TIME));
      jsonKey.put(DimensionsDescriptor.DIMENSION_TIME_BUCKET, TimeBucket.values()[key.getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET)].getText());
    }

    for (String field : fieldList) {
      if (!field.equals(DimensionsDescriptor.DIMENSION_TIME) && !field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        jsonKey.put(field, key.getField(field));
      }
    }

    return jsonKey;
  }

  private void deleteFromHDHT(EventKey evKey)
  {
    try {
      cache.remove(evKey);
      byte[] gaeKey = getEventKeyBytesGAE(evKey);
      Slice keySlice = new Slice(gaeKey, 0, gaeKey.length);
      delete(getBucketID(), keySlice);
    } catch (IOException e) {
      logger.error("Failed to delete event key: {}. With exception: {}", evKey, e);
    }
  }

  private void populateOTFAggregator(Map<String, Aggregate> aggregatesOfKey, int ddID, GPOMutable key)
  {
    Map<String, FieldsDescriptor> otfAggregators = configurationSchema.getDimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor().get(ddID);

    for (Map.Entry<String, FieldsDescriptor> entry : otfAggregators.entrySet()) {
      String otfAggregatorName = entry.getKey();
      OTFAggregator aggregator = configurationSchema.getAggregatorRegistry().getNameToOTFAggregators().get(otfAggregatorName);
      List<String> childAggregators = configurationSchema.getAggregatorRegistry().getOTFAggregatorToIncrementalAggregators().get(otfAggregatorName);

      List<GPOMutable> mutableChildAggregates = Lists.newArrayList();

      for (String childAgg : childAggregators) {
        mutableChildAggregates.add(aggregatesOfKey.get(childAgg).getAggregates());
      }

      GPOMutable result = aggregator.aggregate(mutableChildAggregates.toArray(new GPOMutable[mutableChildAggregates.size()]));
      EventKey evKey = new EventKey(getSchemaID(), ddID, staticOTFAggID, key);
      aggregatesOfKey.put(otfAggregatorName, new Aggregate(evKey, result));
    }
  }

  private String getAggregatorNameFromId(int id)
  {
    if (aggregatorIDToNameMap == null) {
      aggregatorIDToNameMap = Maps.newHashMap();

      for (Map.Entry<String, Integer> entry : this.configurationSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().entrySet()) {
        aggregatorIDToNameMap.put(entry.getValue(), entry.getKey());
      }
    }

    return aggregatorIDToNameMap.get(id);
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> definePartitions(Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> partitions, PartitioningContext context)
  {
    Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> newPartitions = super.definePartitions(partitions, context);
    DefaultPartition.assignPartitionKeys(newPartitions, this.finalizedKey);

    return newPartitions;
  }

  public final transient DefaultInputPort<KeyTracker.FinalizedKey> finalizedKey = new DefaultInputPort<KeyTracker.FinalizedKey>()
  {
    @Override
    public void process(KeyTracker.FinalizedKey tuple)
    {
      processFinalizedKeys(tuple);
    }
  };

  @Override
  public int getPartitionGAE(Aggregate inputEvent)
  {
    return inputEvent.hashCode();
  }

  protected void processFinalizedKeys(KeyTracker.FinalizedKey tuple)
  {
    FieldsDescriptor keyDescriptor = this.configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(tuple.getDdID());
    tuple.getKey().setFieldDescriptor(keyDescriptor);
    emitFinalizedData(tuple.getDdID(), tuple.getKey());
  }

  @VisibleForTesting
  protected DimensionalConfigurationSchema getConfigurationSchema()
  {
    return this.configurationSchema;
  }

  @VisibleForTesting
  protected Map<EventKey, Aggregate> getCache()
  {
    return this.cache;
  }

  public void setDeleteFinalizedData(boolean deleteFinalizedData)
  {
    this.deleteFinalizedData = deleteFinalizedData;
  }
}
