/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsConversionContext;
import org.apache.apex.malhar.lib.dimensions.DimensionsDescriptor;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.apex.malhar.lib.dimensions.aggregator.AbstractTopBottomAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.CompositeAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.OTFAggregator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.dimensions.AggregationIdentifier;
import com.datatorrent.netlet.util.Slice;

/**
 * This operator is a base class for dimension store operators. This operator assumes that an upstream
 * {@link com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema} operator is producing
 * {@link Aggregate} objects which are provided to it as input.
 *
 * @since 3.1.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class DimensionsStoreHDHT extends AbstractSinglePortHDHTWriter<Aggregate>
{
  /**
   * The default number of windows for which data is held in the cache.
   */
  public static final int DEFAULT_CACHE_WINDOW_DURATION = 120;
  /**
   * This is the key value to use for storing the committed windowID in a bucket.
   */
  public static final int META_DATA_ID_WINDOW_ID = 0;
  /**
   * This is the key for storing committed window IDs as a {@link Slice}.
   */
  public static final Slice WINDOW_ID_KEY = new Slice(GPOUtils.serializeInt(META_DATA_ID_WINDOW_ID));
  /**
   * This is the key used to store the format version of the store.
   */
  public static final int META_DATA_ID_STORE_FORMAT = 1;
  /**
   * This is the key used to store the format version of the store, as a {@link Slice}.
   */
  public static final Slice STORE_FORMAT_KEY = new Slice(GPOUtils.serializeInt(META_DATA_ID_STORE_FORMAT));
  /**
   * This is the store format version to store in each HDHT bucket. The store format version represents
   * the way in which data is serialized to HDHT. If this format changes in the future, existing data
   * could be read out of HDHT and reformated, to support seamless upgrades.
   */
  public static final int STORE_FORMAT_VERSION = 0;
  /**
   * This is the byte representation of the current Store Format Version.
   */
  public static final byte[] STORE_FORMAT_VERSION_BYTES = GPOUtils.serializeInt(STORE_FORMAT_VERSION);
  /**
   * The number of windows that the operator's {@link Aggregate} cache is preserved for.
   */
  @Min(1)
  private int cacheWindowDuration = DEFAULT_CACHE_WINDOW_DURATION;
  /**
   * This keeps track of the number of windows seen since the last time the operator's cache
   * was cleared.
   */
  private int cacheWindowCount = 0;
  /**
   * The ID of the current window that this operator is on.
   */
  @VisibleForTesting
  protected transient long currentWindowID;
  /**
   * This is the operator's {@link Aggregate} cache, which is used to store aggregations.
   * The keys of this map are {@link EventKey}s for this aggregate. The values in this
   * map are the corresponding {@link Aggregate}s.
   */
  protected transient Map<EventKey, Aggregate> cache = new ConcurrentHashMap<EventKey, Aggregate>();

  /**
   * The computation for composite aggregators need to get the aggregates of embed incremental aggregator.
   * This embed aggregator can identified by AggregationIdentifier
   */
  protected transient Map<AggregationIdentifier, Set<EventKey>> embedIdentifierToEventKeys;

  /**
   * A map from composite aggregator ID to aggregate value.
   * not use ConcurrentHashMap as all operations are in same thread,
   */
  protected transient Map<Integer, GPOMutable> compositeAggregteCache = Maps.newHashMap();
  /**
   * The IDs of the HDHT buckets that this operator writes to.
   */
  protected Set<Long> buckets = Sets.newHashSet();
  /**
   * This flag indicates whether or not meta information like committed window ID, and
   * storage format version have been read from each bucket.
   */
  @VisibleForTesting
  protected transient boolean readMetaData = false;
  /**
   * This {@link Map} holds the committed window ID for each HDHT bucket. This map is used
   * to determine when it is acceptable to start writing data to an HDHT bucket for fault
   * tolerance purposes.
   */
  @VisibleForTesting
  protected final transient Map<Long, Long> futureBuckets = Maps.newHashMap();

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Aggregate> updates = new DefaultOutputPort<>();

  private boolean useSystemTimeForLatestTimeBuckets = false;

  private Long minTimestamp = null;
  private Long maxTimestamp = null;

  private final transient GPOByteArrayList bal = new GPOByteArrayList();
  private final transient GPOByteArrayList tempBal = new GPOByteArrayList();

  protected Aggregate tmpCompositeDestAggregate = new Aggregate();
  /**
   * Constructor to create operator.
   */
  public DimensionsStoreHDHT()
  {
    //Do nothing
  }

  /**
   * This is a helper method that is used to retrieve the aggregator ID corresponding to an aggregatorName.
   *
   * @param aggregatorName Name of the aggregator to look up an ID for.
   * @return The ID of the aggregator corresponding to the given aggregatorName.
   */
  protected abstract int getIncrementalAggregatorID(String aggregatorName);

  /**
   * This is a helper method which gets  the {@link IncrementalAggregator} corresponding to the given aggregatorID.
   *
   * @param aggregatorID The aggregatorID of the {@link IncrementalAggregator} to retrieve.
   * @return The {@link IncrementalAggregator} with the given ID.
   */
  protected abstract IncrementalAggregator getAggregator(int aggregatorID);

  protected abstract CompositeAggregator getCompositeAggregator(int aggregatorID);

  /**
   * This is a helper method which gets the {@link FieldsDescriptor} object for the key corresponding to the given
   * schema and
   * {@link DimensionsDescriptor} in that schema.
   *
   * @param schemaID               The schemaID corresponding to the schema for which to retrieve a key descriptor.
   * @param dimensionsDescriptorID The dimensionsDescriptorID corresponding to the {@link DimensionsDescriptor} for
   *                               which to retrieve a key descriptor.
   * @return The key descriptor for the given schemaID and dimensionsDescriptorID.
   */
  protected abstract FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID);

  /**
   * This is a helper method which gets the {@link FieldsDescriptor} object for the aggregates corresponding to the
   * given schema,
   * {@link DimensionsDescriptor} in that schema, and aggregtorID.
   *
   * @param schemaID               The schemaID corresponding to the schema for which to retrieve a value descriptor.
   * @param dimensionsDescriptorID The dimensionsDescriptorID corresponding to the {@link DimensionsDescriptor} for
   *                               which
   *                               to retrieve a value descriptor.
   * @param aggregatorID           The id of the aggregator used to aggregate the values.
   * @return The value descriptor for the given schemaID, dimensionsDescriptorID, and aggregatorID.
   */
  protected abstract FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID);

  /**
   * This is a helper method which retrieves the bucketID that a schema corresponds to.
   *
   * @param schemaID The schema ID for which to find a bucketID.
   * @return The bucketID corresponding to the given schemaID.
   */
  protected abstract long getBucketForSchema(int schemaID);

  /**
   * This is another helper method which gets the bucket that the given {@link EventKey} belongs to.
   *
   * @param eventKey The event key.
   * @return The bucketID of the bucket that the given {@link EventKey} belongs to.
   */
  protected long getBucketForSchema(EventKey eventKey)
  {
    return getBucketForSchema(eventKey.getSchemaID());
  }

  /**
   * This is a convenience helper method which serializes the key of the given {@link Aggregate}.
   *
   * @param gae The {@link Aggregate} to serialize.
   * @return The serialized {@link Aggregate}.
   */
  protected byte[] getKeyBytesGAE(Aggregate gae)
  {
    return getEventKeyBytesGAE(gae.getEventKey());
  }

  /**
   * Method serializes the given {@link EventKey}.
   *
   * @param eventKey The {@link EventKey} to serialize.
   * @return The serialized {@link EventKey}.
   */
  public synchronized byte[] getEventKeyBytesGAE(EventKey eventKey)
  {
    long timestamp = 0;

    if (eventKey.getKey()
        .getFieldDescriptor().getFieldList()
        .contains(DimensionsDescriptor.DIMENSION_TIME)) {
      //If key includes a time stamp retrieve it.
      timestamp = eventKey.getKey().getFieldLong(DimensionsDescriptor.DIMENSION_TIME);
    }

    //Time is a special case for HDHT all keys should be prefixed by a timestamp.
    byte[] timeBytes = Longs.toByteArray(timestamp);
    byte[] schemaIDBytes = Ints.toByteArray(eventKey.getSchemaID());
    byte[] dimensionDescriptorIDBytes = Ints.toByteArray(eventKey.getDimensionDescriptorID());
    byte[] aggregatorIDBytes = Ints.toByteArray(eventKey.getAggregatorID());
    byte[] gpoBytes = GPOUtils.serialize(eventKey.getKey(), tempBal);

    bal.add(timeBytes);
    bal.add(schemaIDBytes);
    bal.add(dimensionDescriptorIDBytes);
    bal.add(aggregatorIDBytes);
    bal.add(gpoBytes);

    byte[] serializedBytes = bal.toByteArray();
    bal.clear();

    return serializedBytes;
  }

  /**
   * This method serializes the aggregate payload ({@link GPOMutable}) in the given {@link Aggregate}.
   *
   * @param event The {@link Aggregate} whose aggregate payload needs to be serialized.
   * @return The serialized aggregate payload of the given {@link Aggregate}.
   */
  public synchronized byte[] getValueBytesGAE(Aggregate event)
  {
    FieldsDescriptor metaDataDescriptor = null;
    final int aggregatorID = event.getEventKey().getAggregatorID();
    if (getAggregator(aggregatorID) != null) {
      metaDataDescriptor = getAggregator(aggregatorID).getMetaDataDescriptor();
    } else {
      metaDataDescriptor = getCompositeAggregator(aggregatorID).getMetaDataDescriptor();
    }

    if (metaDataDescriptor != null) {
      bal.add(GPOUtils.serialize(event.getMetaData(), tempBal));
    }

    bal.add(GPOUtils.serialize(event.getAggregates(), tempBal));
    byte[] serializedBytes = bal.toByteArray();
    bal.clear();

    return serializedBytes;
  }

  /**
   * Creates an {@link Aggregate} from a serialized {@link EventKey} and a
   * serialize {@link GPOMutable} object.
   *
   * @param key       A serialized {@link EventKey}.
   * @param aggregate A serialized {@link GPOMutable} containing all the values of the aggregates.
   * @return An {@link Aggregate} object with the given {@link EventKey} and aggregate payload.
   */
  public Aggregate fromKeyValueGAE(Slice key, byte[] aggregate)
  {
    MutableInt offset = new MutableInt(Type.LONG.getByteSize());
    int schemaID = GPOUtils.deserializeInt(key.buffer,
        offset);
    int dimensionDescriptorID = GPOUtils.deserializeInt(key.buffer,
        offset);
    int aggregatorID = GPOUtils.deserializeInt(key.buffer,
        offset);

    FieldsDescriptor keysDescriptor = getKeyDescriptor(schemaID, dimensionDescriptorID);
    FieldsDescriptor aggDescriptor = getValueDescriptor(schemaID, dimensionDescriptorID, aggregatorID);

    FieldsDescriptor metaDataDescriptor = null;
    if (getAggregator(aggregatorID) != null) {
      metaDataDescriptor = getAggregator(aggregatorID).getMetaDataDescriptor();
    } else {
      metaDataDescriptor = getCompositeAggregator(aggregatorID).getMetaDataDescriptor();
    }

    GPOMutable keys = GPOUtils.deserialize(keysDescriptor, key.buffer, offset);
    offset.setValue(0);

    GPOMutable metaData = null;

    if (metaDataDescriptor != null) {
      metaData = GPOUtils.deserialize(metaDataDescriptor, aggregate, offset);
      metaData.applyObjectPayloadFix();
    }

    GPOMutable aggs = GPOUtils.deserialize(aggDescriptor, aggregate, offset);

    Aggregate gae = new Aggregate(keys,
        aggs,
        metaData,
        schemaID,
        dimensionDescriptorID,
        aggregatorID);
    return gae;
  }

  /**
   * This is a helper method which synchronously loads data from the given key from the given
   * bucketID. This method performs the same operation as the other {@link #load(long, com.datatorrent.netlet.util
   * .Slice)
   * method except, it deserializes the value byte array into an {@link Aggregate}.
   *
   * @param eventKey The {@link EventKey} whose corresponding {@link Aggregate} needs to be loaded.
   * @return The {@link Aggregate} corresponding to the given {@link EventKey}.
   */
  public Aggregate load(EventKey eventKey)
  {
    long bucket = getBucketForSchema(eventKey);
    byte[] key = getEventKeyBytesGAE(eventKey);

    Slice keySlice = new Slice(key, 0, key.length);
    byte[] val = load(bucket, keySlice);

    if (val == null) {
      return null;
    }

    return fromKeyValueGAE(keySlice, val);
  }

  /**
   * This is a helper method which synchronously loads data from with the given key from
   * the given bucketID. This method first checks the uncommitted cache of the operator
   * before attempting to load the data from HDFS.
   *
   * @param bucketID The bucketID from which to load data.
   * @param keySlice The key for which to load data.
   * @return The value of the data with the given key.
   */
  public byte[] load(long bucketID, Slice keySlice)
  {
    byte[] val = getUncommitted(bucketID, keySlice);

    if (val == null) {
      try {
        val = get(bucketID, keySlice);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return val;
  }

  /**
   * This method determines the partitionID that the given {@link Aggregate} belongs to. This method
   * is called by the operator's stream codec.
   *
   * @param inputEvent The {@link Aggregate} whose partitionID needs to be determined.
   * @return The id of the partition that the given {@link Aggregate} belongs to.
   */
  public int getPartitionGAE(Aggregate inputEvent)
  {
    return inputEvent.getBucketID();
  }

  /**
   * This method stores the given {@link Aggregate} into HDHT.
   *
   * @param gae The {@link Aggregate} to store into HDHT.
   */
  public void putGAE(Aggregate gae)
  {
    try {
      put(getBucketForSchema(gae.getSchemaID()),
          new Slice(codec.getKeyBytes(gae)),
          codec.getValueBytes(gae));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * This is a helper method which writes out the store's format version.
   *
   * @param bucket The bucketID to write out the store's format version to.
   * @throws IOException
   */
  public void putStoreFormatVersion(long bucket) throws IOException
  {
    put(bucket, STORE_FORMAT_KEY, STORE_FORMAT_VERSION_BYTES);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowID = windowId;

    super.beginWindow(windowId);

    if (!readMetaData) {
      //Note reading only seems to work between begin and end window in the unit tests.
      //This could should only be executed once when the operator starts.

      //read meta data such as committed windowID when the operator starts.
      for (Long bucket : buckets) {
        byte[] windowIDValueBytes;

        //loads the committed windowID from the bucket.
        windowIDValueBytes = load(bucket, WINDOW_ID_KEY);

        if (windowIDValueBytes == null) {
          continue;
        }

        long committedWindowID = GPOUtils.deserializeLong(windowIDValueBytes, new MutableInt(0));
        futureBuckets.put(bucket, committedWindowID);
      }

      //Write Store Format Version out to each bucket
      for (Long bucket : buckets) {
        try {
          LOG.debug("Writing out store format version to bucket {}", bucket);
          putStoreFormatVersion(bucket);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      readMetaData = true;
    }

    initializeEmbedIdentifierToEventKeys();
  }

  @Override
  protected void processEvent(Aggregate gae)
  {
    LOG.debug("Before event key {}", gae.getEventKey());

    int schemaID = gae.getSchemaID();
    int ddID = gae.getDimensionDescriptorID();
    int aggregatorID = gae.getAggregatorID();

    AggregationIdentifier aggregationIdentifier = new AggregationIdentifier(schemaID, ddID, aggregatorID);
    Set<EventKey> embedEventKeys = embedIdentifierToEventKeys.get(aggregationIdentifier);

    FieldsDescriptor keyFieldsDescriptor = getKeyDescriptor(schemaID, ddID);
    FieldsDescriptor valueFieldsDescriptor = getValueDescriptor(schemaID, ddID, aggregatorID);

    gae.getKeys().setFieldDescriptor(keyFieldsDescriptor);

    if (valueFieldsDescriptor == null) {
      LOG.info("ids for failure {} {} {}", schemaID, ddID, aggregatorID);
    }

    gae.getAggregates().setFieldDescriptor(valueFieldsDescriptor);

    //Skip data for buckets with greater committed window Ids
    if (!futureBuckets.isEmpty()) {
      long bucket = getBucketForSchema(schemaID);
      Long committedWindowID = futureBuckets.get(bucket);

      if (committedWindowID != null &&
          currentWindowID <= committedWindowID) {
        LOG.debug("Skipping");
        return;
      }
    }

    GPOMutable metaData = gae.getMetaData();

    IncrementalAggregator aggregator = getAggregator(gae.getAggregatorID());

    if (metaData != null) {
      metaData.setFieldDescriptor(aggregator.getMetaDataDescriptor());
      metaData.applyObjectPayloadFix();
    }

    LOG.debug("Event key {}", gae.getEventKey());

    Aggregate aggregate = cache.get(gae.getEventKey());

    if (aggregate == null) {
      aggregate = load(gae.getEventKey());

      if (aggregate != null) {
        cache.put(aggregate.getEventKey(), aggregate);
        if (embedEventKeys != null) {
          embedEventKeys.add(aggregate.getEventKey());
        }
      }
    }

    if (aggregate == null) {
      cache.put(gae.getEventKey(), gae);
      if (embedEventKeys != null) {
        embedEventKeys.add(gae.getEventKey());
      }
    } else {
      LOG.debug("Aggregating input");
      aggregator.aggregate(aggregate, gae);
      if (embedEventKeys != null) {
        embedEventKeys.add(gae.getEventKey());
      }
    }
  }

  @Override
  public void endWindow()
  {
    //Write out the last committed window ID for each bucket.
    byte[] currentWindowIDBytes = GPOUtils.serializeLong(currentWindowID);

    for (Long bucket : buckets) {
      Long committedWindowID = futureBuckets.get(bucket);

      if (committedWindowID == null ||
          committedWindowID <= currentWindowID) {
        futureBuckets.remove(bucket);

        try {
          put(bucket, WINDOW_ID_KEY, currentWindowIDBytes);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    cacheWindowCount++;

    handleTopBottomAggregators();

    //Write out the contents of the cache.
    for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
      putGAE(entry.getValue());
    }

    emitUpdates();

    if (cacheWindowCount == cacheWindowDuration) {
      //clear the cache if the cache window duration is reached.
      cache.clear();
      cacheWindowCount = 0;
    }

    cleanupEmbedIdentifierToEventKeys();

    super.endWindow();
  }

  /**
   * This method initialize the cacheForCompositeComputation.
   * Basically, it computes the AggregationIdentifier for all composite aggregators and put as the key of
   * cacheForCompositeComputation
   */
  protected void initializeEmbedIdentifierToEventKeys()
  {
    if (embedIdentifierToEventKeys != null) {
      return;
    }

    embedIdentifierToEventKeys = Maps.newHashMap();
    Map<Integer, AbstractTopBottomAggregator> topBottomAggregatorIdToInstance = getTopBottomAggregatorIdToInstance();
    if (topBottomAggregatorIdToInstance != null) {
      Set<AggregationIdentifier> allIdentifiers = Sets.newHashSet();

      for (Map.Entry<Integer, AbstractTopBottomAggregator> entry : topBottomAggregatorIdToInstance.entrySet()) {
        allIdentifiers.addAll(getDependedIncrementalAggregationIdentifiers(entry.getValue()));
      }

      for (AggregationIdentifier identifier : allIdentifiers) {
        embedIdentifierToEventKeys.put(identifier, Sets.<EventKey>newHashSet());
      }
    }
  }

  /**
   * only cleanup the value. keep the entry for next time
   */
  protected void cleanupEmbedIdentifierToEventKeys()
  {
    if (embedIdentifierToEventKeys == null) {
      return;
    }

    for (Set<EventKey> eventKeys : embedIdentifierToEventKeys.values()) {
      eventKeys.clear();
    }
  }

  /**
   * in case of embed is OTF aggregator, get identifier for incremental aggregators
   * @param topBottomAggregator
   * @return
   */
  protected abstract Set<AggregationIdentifier> getDependedIncrementalAggregationIdentifiers(
      AbstractTopBottomAggregator topBottomAggregator);

  /**
   * input: the aggregte of Incremental Aggregators from cache
   * output: create a new cache instead of share the same cache of Incremental Aggregators
   */
  protected void handleTopBottomAggregators()
  {
    Map<Integer, AbstractTopBottomAggregator> topBottomAggregatorIdToInstance = getTopBottomAggregatorIdToInstance();
    if (topBottomAggregatorIdToInstance == null) {
      return;
    }
    
    for (AbstractTopBottomAggregator aggregator : topBottomAggregatorIdToInstance.values()) {
      Set<AggregationIdentifier> embedAggregatorIdentifiers = getDependedIncrementalAggregationIdentifiers(aggregator);
      String embedAggregatorName = aggregator.getEmbedAggregatorName();
      if (isIncrementalAggregator(embedAggregatorName)) {
        //embed is incremental aggregator
        for (AggregationIdentifier embedAggregatorIdentifier : embedAggregatorIdentifiers) {
          Set<EventKey> eventKeysForIdentifier = embedIdentifierToEventKeys.get(embedAggregatorIdentifier);
          if (eventKeysForIdentifier.isEmpty()) {
            continue;
          }

          //group the event key
          Map<EventKey, Set<EventKey>> compositeEventKeyToEmbedEventKeys =
              groupEventKeysByCompositeEventKey(aggregator, eventKeysForIdentifier);
          for (Map.Entry<EventKey, Set<EventKey>> compositeEventKeyEntry :
              compositeEventKeyToEmbedEventKeys.entrySet()) {
            aggregateComposite(aggregator, compositeEventKeyEntry.getKey(), compositeEventKeyEntry.getValue(), cache);
          }
        }
      } else {
        //embed is an oft aggregator
        Map<EventKey, Aggregate> oftEventKeyToAggregate =
            computeOTFAggregates(aggregator, embedAggregatorName, getOTFAggregatorByName(embedAggregatorName));

        //group the event by composite event key
        Map<EventKey, Set<EventKey>> compositeEventKeyToEmbedEventKeys =
            groupEventKeysByCompositeEventKey(aggregator, oftEventKeyToAggregate.keySet());

        for (Map.Entry<EventKey, Set<EventKey>> compositeEventKeyEntry :
            compositeEventKeyToEmbedEventKeys.entrySet()) {
          aggregateComposite(aggregator, compositeEventKeyEntry.getKey(), compositeEventKeyEntry.getValue(),
              oftEventKeyToAggregate);
        }
      }
    }
  }


  protected abstract boolean isIncrementalAggregator(String aggregatorName);

  protected abstract OTFAggregator getOTFAggregatorByName(String otfAggregatorName);

  /**
   * group the event keys of embed aggregator by event key of composite aggregator.
   * The composite event keys should have less fields than the embed aggregator event keys
   *
   * @param aggregator
   * @param embedAggregatorEventKeys
   * @return
   */
  protected Map<EventKey, Set<EventKey>> groupEventKeysByCompositeEventKey(
      AbstractTopBottomAggregator compositeAggregator, Set<EventKey> embedAggregatorEventKeys)
  {
    Map<EventKey, Set<EventKey>> groupedEventKeys = Maps.newHashMap();

    for (EventKey embedEventKey : embedAggregatorEventKeys) {
      EventKey compositeEventKey = createCompositeEventKey(compositeAggregator, embedEventKey);
      Set<EventKey> embedEventKeys = groupedEventKeys.get(compositeEventKey);
      if (embedEventKeys == null) {
        embedEventKeys = Sets.newHashSet();
        groupedEventKeys.put(compositeEventKey, embedEventKeys);
      }
      embedEventKeys.add(embedEventKey);
    }
    return groupedEventKeys;
  }

  /**
   * create the event key for the composite aggregate based on the event key of embed aggregator.
   * @param compositeAggregator
   * @param embedEventKey
   * @return
   */
  protected EventKey createCompositeEventKey(AbstractTopBottomAggregator compositeAggregator, EventKey embedEventKey)
  {
    return createCompositeEventKey(embedEventKey.getBucketID(), embedEventKey.getSchemaID(),
        compositeAggregator.getDimensionDescriptorID(), compositeAggregator.getAggregatorID(),
        compositeAggregator.getFields(),
        embedEventKey);
  }

  /**
   * The composite event keys should have less fields than the embed aggregator event keys
   * @param bucketId
   * @param schemaId
   * @param dimensionDescriptorId
   * @param aggregatorId
   * @param compositeFieldNames
   * @param embedEventKey
   * @return
   */
  protected EventKey createCompositeEventKey(int bucketId, int schemaId, int dimensionDescriptorId, int aggregatorId,
      Set<String> compositeFieldNames, EventKey embedEventKey)
  {
    final GPOMutable embedKey = embedEventKey.getKey();

    GPOMutable key = cloneGPOMutableLimitToFields(embedKey, compositeFieldNames);

    return new EventKey(bucketId, schemaId, dimensionDescriptorId, aggregatorId, key);
  }

  /**
   * clone a GPOMutable with selected field name
   * TODO: this class can move to the GPOUtils
   * @param fieldNames
   * @return
   */
  public static GPOMutable cloneGPOMutableLimitToFields(GPOMutable orgGpo, Set<String> fieldNames)
  {
    Set<String> fieldsIncludeTime = Sets.newHashSet();
    fieldsIncludeTime.addAll(fieldNames);
    fieldsIncludeTime.add("timeBucket");
    fieldsIncludeTime.add("time");

    return new GPOMutable(orgGpo, new Fields(fieldsIncludeTime));
  }

  protected Aggregate fetchOrLoadAggregate(EventKey eventKey)
  {
    Aggregate aggregate = cache.get(eventKey);
    if (aggregate != null) {
      return aggregate;
    }

    aggregate = load(eventKey);

    if (aggregate != null) {
      cache.put(eventKey, aggregate);
    }
    return aggregate;
  }

  //this is a tmp map use member variable just for share the variable
  protected transient Map<EventKey, Aggregate> eventKeyToAggregate = Maps.newHashMap();
  protected Map<EventKey, Aggregate> getAggregates(Set<EventKey> eventKeys)
  {
    eventKeyToAggregate.clear();
    for (EventKey eventKey : eventKeys) {
      eventKeyToAggregate.put(eventKey, cache.get(eventKey));
    }
    return eventKeyToAggregate;
  }

  /**
   *
   * @param aggregator The composite aggregator for the aggregation
   * @param compositeEventKey The composite event key, used to locate the target/dest aggregate
   * @param inputEventKeys The input(incremental) event keys, used to locate the input aggregates
   * @param inputEventKeyToAggregate The repository of input event key to aggregate. inputEventKeyToAggregate.keySet()
   * should be a super set of inputEventKeys
   */
  protected void aggregateComposite(AbstractTopBottomAggregator aggregator, EventKey compositeEventKey,
      Set<EventKey> inputEventKeys, Map<EventKey, Aggregate> inputEventKeyToAggregate )
  {
    Aggregate resultAggregate = fetchOrLoadAggregate(compositeEventKey);

    if (resultAggregate == null) {
      resultAggregate = new Aggregate(compositeEventKey,  new GPOMutable(aggregator.getAggregateDescriptor()));
      cache.put(compositeEventKey, resultAggregate);
    }

    aggregator.aggregate(resultAggregate, inputEventKeys, inputEventKeyToAggregate);
  }


  public Aggregate createAggregate(EventKey eventKey,
      DimensionsConversionContext context,
      int aggregatorIndex)
  {
    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);

    Aggregate aggregate = new Aggregate(eventKey, aggregates);
    aggregate.setAggregatorIndex(aggregatorIndex);

    return aggregate;
  }

  protected abstract List<String> getOTFChildrenAggregatorNames(String oftAggregatorName);


  /**
   * compute to get the result of OTF aggregates(result). The input aggregate value from the cache.
   * @param aggregator the composite aggregator of this OTF aggregator
   * @param oftAggregatorName
   * @param oftAggregator
   * @return
   */
  protected Map<EventKey, Aggregate> computeOTFAggregates(AbstractTopBottomAggregator aggregator,
      String oftAggregatorName, OTFAggregator oftAggregator)
  {
    Set<AggregationIdentifier> dependedIncrementalAggregatorIdentifiers =
        getDependedIncrementalAggregationIdentifiers(aggregator);
    List<String> childrenAggregator = getOTFChildrenAggregatorNames(oftAggregatorName);
    Map<Integer, Integer> childAggregatorIdToIndex = Maps.newHashMap();
    int index = 0;
    for (String childAggregatorName : childrenAggregator) {
      childAggregatorIdToIndex.put(this.getIncrementalAggregatorID(childAggregatorName), index++);
    }

    //get event keys for children of OTF
    List<Set<EventKey>> childrenEventKeysByAggregator = Lists.newArrayList();
    for (AggregationIdentifier identifier : dependedIncrementalAggregatorIdentifiers) {
      Set<EventKey> eventKeys = embedIdentifierToEventKeys.get(identifier);
      if (eventKeys != null && !eventKeys.isEmpty()) {
        childrenEventKeysByAggregator.add(eventKeys);
      }
    }

    if (childrenEventKeysByAggregator.isEmpty()) {
      return Collections.emptyMap();
    }

    //arrange the EventKey by key value, the OTF requires the aggregates for same event key for computing.
    List<List<EventKey>> childrenEventKeysByKeyValue = Lists.newArrayList();
    Set<EventKey> eventKeys = childrenEventKeysByAggregator.get(0);
    for (EventKey eventKey : eventKeys) {
      List<EventKey> eventKeysOfOneKeyValue = Lists.newArrayList();
      eventKeysOfOneKeyValue.add(eventKey);
      //the eventKey is from list(0)
      //get one from one entry of the list with same key
      final GPOMutable key = eventKey.getKey();
      for (int i = 1; i < childrenEventKeysByAggregator.size(); ++i) {
        Set<EventKey> eventKeysOfOneAggregator = childrenEventKeysByAggregator.get(i);
        if (eventKeysOfOneAggregator == null) {
          continue;
        }

        EventKey matchedEventKey = null;
        for (EventKey ek : eventKeysOfOneAggregator) {
          if (key.equals(ek.getKey())) {
            matchedEventKey = ek;
            break;
          }
        }
        if (matchedEventKey == null) {
          //can't find matchedEventKey, ignore
          continue;
        }
        eventKeysOfOneKeyValue.add(matchedEventKey);
      }
      if (eventKeysOfOneKeyValue.size() != childrenAggregator.size()) {
        LOG.warn("The fetched size is " + eventKeysOfOneKeyValue.size() + ", not same as expected size " +
            childrenAggregator.size());
      } else {
        childrenEventKeysByKeyValue.add(eventKeysOfOneKeyValue);
      }
    }

    Map<EventKey, Aggregate> compositeInputAggregates = Maps.newHashMap();
    GPOMutable[] srcValues = new GPOMutable[childrenEventKeysByKeyValue.get(0).size()];

    for (List<EventKey> sameKeyEvents: childrenEventKeysByKeyValue) {
      for (EventKey ek : sameKeyEvents) {
        //the values pass to the oft aggregator should be ordered by the depended aggregators
        srcValues[childAggregatorIdToIndex.get(ek.getAggregatorID())] = cache.get(ek).getAggregates();
      }
      GPOMutable result = oftAggregator.aggregate(srcValues);
      compositeInputAggregates.put(sameKeyEvents.get(0), new Aggregate(sameKeyEvents.get(0), result));
    }

    return compositeInputAggregates;
  }

  /**
   * get all composite aggregators
   * @return Map of aggregator id to top bottom aggregator
   */
  protected abstract Map<Integer, AbstractTopBottomAggregator> getTopBottomAggregatorIdToInstance();


  /**
   * This method is called in {@link #endWindow} and emits updated aggregates. Override
   * this method if you want to control whether or not updates are emitted.
   */
  protected void emitUpdates()
  {
    if (updates.isConnected()) {
      for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
        updates.emit(entry.getValue());
      }
    }
  }

  @Override
  public HDHTCodec<Aggregate> getCodec()
  {
    return new GenericAggregateEventCodec();
  }

  /**
   * Returns the cacheWindowDuration.
   *
   * @return The cacheWindowDuration.
   */
  public int getCacheWindowDuration()
  {
    return cacheWindowDuration;
  }

  /**
   * Sets the cacheWindowDuration which determines the number of windows for which
   * data is held in this operator's cache.
   *
   * @param cacheWindowDuration The number of windows for which data is held in this operator's cache.
   */
  public void setCacheWindowDuration(int cacheWindowDuration)
  {
    this.cacheWindowDuration = cacheWindowDuration;
  }

  /**
   * @return the minTimestamp
   */
  @Unstable
  public Long getMinTimestamp()
  {
    return minTimestamp;
  }

  /**
   * @param minTimestamp the minTimestamp to set
   */
  @Unstable
  public void setMinTimestamp(Long minTimestamp)
  {
    this.minTimestamp = minTimestamp;
  }

  /**
   * @return the maxTimestamp
   */
  @Unstable
  public Long getMaxTimestamp()
  {
    return maxTimestamp;
  }

  /**
   * @param maxTimestamp the maxTimestamp to set
   */
  @Unstable
  public void setMaxTimestamp(Long maxTimestamp)
  {
    this.maxTimestamp = maxTimestamp;
  }

  /**
   * @return the useSystemTimeForLatestTimeBuckets
   */
  @Unstable
  public boolean isUseSystemTimeForLatestTimeBuckets()
  {
    return useSystemTimeForLatestTimeBuckets;
  }

  /**
   * @param useSystemTimeForLatestTimeBuckets the useSystemTimeForLatestTimeBuckets to set
   */
  @Unstable
  public void setUseSystemTimeForLatestTimeBuckets(boolean useSystemTimeForLatestTimeBuckets)
  {
    this.useSystemTimeForLatestTimeBuckets = useSystemTimeForLatestTimeBuckets;
  }

  /**
   * This is a codec which defines how data is serialized to HDHT. This codec is effectively
   * a proxy which call's on the operator's overridable {@link #getKeyBytesGAE}, {@link #getValueBytesGAE},
   * {@link #fromKeyValueGAE}, and {@link #getPartitionGAE} methods.
   */
  class GenericAggregateEventCodec extends KryoSerializableStreamCodec<Aggregate>
      implements HDHTCodec<Aggregate>
  {
    private static final long serialVersionUID = 201503170256L;

    /**
     * Creates the codec.
     */
    public GenericAggregateEventCodec()
    {
      //Do nothing
    }

    @Override
    public byte[] getKeyBytes(Aggregate gae)
    {
      return getKeyBytesGAE(gae);
    }

    @Override
    public byte[] getValueBytes(Aggregate gae)
    {
      return getValueBytesGAE(gae);
    }

    @Override
    public Aggregate fromKeyValue(Slice key, byte[] value)
    {
      return fromKeyValueGAE(key, value);
    }

    @Override
    public int getPartition(Aggregate gae)
    {
      return getPartitionGAE(gae);
    }
  }

  @Override
  public void addQuery(HDSQuery query)
  {
    super.addQuery(query);
  }

  /**
   * Gets the currently issued {@link HDSQuery}s.
   *
   * @return The currently issued {@link HDSQuery}s.
   */
  public Map<Slice, HDSQuery> getQueries()
  {
    return this.queries;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsStoreHDHT.class);
}
