/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation.regression;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import net.sf.jagg.AggregateFunction;
import net.sf.jagg.Aggregations;
import net.sf.jagg.Aggregator;
import net.sf.jagg.model.AggregateValue;

public class Validator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(Validator.class);

  private static final Map<String, String> aggMapping;

  static {
    aggMapping = new HashMap<>();
    aggMapping.put("SUM", "SumAggregator");
    aggMapping.put("COUNT", "CountAggregator");
    aggMapping.put("AVG", "AvgAggregator");
  }

  private transient long windowId = 0;
  private transient double windowTimeSec;

  private transient Map<Long, List<POJO>> minPojoStored = new LinkedHashMap<>();
  private transient Map<Long, List<POJO>> hourPojoStored = new LinkedHashMap<>();
  private transient Map<Long, List<POJO>> dayPojoStored = new LinkedHashMap<>();
  private transient Map<Long, List<JSONObject>> minFinalizedData = new LinkedHashMap<>();
  private transient Map<Long, List<JSONObject>> hourFinalizedData = new LinkedHashMap<>();
  private transient Map<Long, List<JSONObject>> dayFinalizedData = new LinkedHashMap<>();

  private transient List<List<String>> combinations = new ArrayList<>();
  private transient Map<String, List<String>> computations = new HashMap<>();
  private transient List<AggregateFunction> allAggList = new ArrayList<>();
  private transient List<String> buckets = new ArrayList<>();

  private transient FileSystem fs;
  private transient FSDataOutputStream out;

  private String groupByKeys;
  private String computeFields;
  private String timeBuckets;
  private int activeAggregationInterval;
  private boolean printFinalizedData = false;

  public transient final DefaultInputPort<POJO> moduleInput = new DefaultInputPort<POJO>()
  {
    @Override
    public void process(POJO tuple)
    {
      processModuleInputTuple(tuple);
    }
  };

  public transient final DefaultInputPort<String> moduleOutput = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processModuleOutputTuple(tuple);
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
    combinations.add(Arrays.asList(new String[]{"timeBucket"}));
    for (String comb : groupByKeys.split(";")) {
      List strings = new ArrayList<>(Arrays.asList(comb.split(",")));
      strings.add("timeBucket");
      combinations.add(strings);
    }

    for (String f : computeFields.split(";")) {
      String[] split = f.split(":");
      for (String s : split[1].split(",")) {
        allAggList.add(Aggregator.getAggregator(aggMapping.get(s) + "(" + split[0] + ")"));
      }
      computations.put(split[0], Arrays.asList(split[1].split(",")));
    }

    for (String s : timeBuckets.split(",")) {
      buckets.add(s);
    }

    Path statusPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + "status");
    Configuration configuration = new Configuration();

    try {
      FileSystem fs = FileSystem.newInstance(statusPath.toUri(), configuration);
      out = fs.create(statusPath, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void teardown()
  {
    try {
      out.close();
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  private void processModuleInputTuple(POJO tuple)
  {
    POJO clone = SerializationUtils.clone(tuple);
    clone.setTimeBucket("1m");
    addToWindowIdBucket(clone, minPojoStored);

    clone = SerializationUtils.clone(tuple);
    clone.setTimeBucket("1h");
    addToWindowIdBucket(clone, hourPojoStored);

    clone = SerializationUtils.clone(tuple);
    clone.setTimeBucket("1d");
    addToWindowIdBucket(clone, dayPojoStored);
  }

  private void addToWindowIdBucket(POJO tuple, Map<Long, List<POJO>> pojoStored)
  {
    List<POJO> list;
    if (pojoStored.containsKey(this.windowId)) {
      list = pojoStored.get(this.windowId);
    } else {
      list = new LinkedList<>();
      pojoStored.put(this.windowId, list);
    }
    list.add(tuple);
  }

  private void processModuleOutputTuple(String tuple)
  {
    try {
      JSONObject jo = new JSONObject(tuple);
      String tb = jo.getJSONObject("groupByKey").getString("timeBucket");
      switch (tb) {
        case "1m":
          addFinalizedStringToWindowIdBucket(jo, minFinalizedData);
          if (printFinalizedData) {
            System.out.println("FinalizedData (min bucket): " + tuple);
          }
          break;
        case "1h":
          addFinalizedStringToWindowIdBucket(jo, hourFinalizedData);
          if (printFinalizedData) {
            System.out.println("FinalizedData (hour bucket): " + tuple);
          }
          break;
        case "1d":
          addFinalizedStringToWindowIdBucket(jo, dayFinalizedData);
          if (printFinalizedData) {
            System.out.println("FinalizedData (day bucket): " + tuple);
          }
          break;
        default:
          throw new RuntimeException("Something went wrong.");
      }
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private void addFinalizedStringToWindowIdBucket(JSONObject tuple, Map<Long, List<JSONObject>> finalizedData)
  {
    List<JSONObject> list;
    if (finalizedData.containsKey(this.windowId)) {
      list = finalizedData.get(this.windowId);
    } else {
      list = new LinkedList<>();
      finalizedData.put(this.windowId, list);
    }
    list.add(tuple);
  }

  @Override
  public void endWindow()
  {
    validateFinalizedData(minFinalizedData, minPojoStored);
    validateFinalizedData(hourFinalizedData, hourPojoStored);
    validateFinalizedData(dayFinalizedData, dayPojoStored);
  }

  private void validateFinalizedData(Map<Long, List<JSONObject>> bucketFinalizedData, Map<Long, List<POJO>> pojoStored)
  {
    Iterator<Map.Entry<Long, List<JSONObject>>> iterator = bucketFinalizedData.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, List<JSONObject>> next = iterator.next();
      long windowId = next.getKey();
      List<JSONObject> finalizedData = next.getValue();
      try {
        validate(windowId, finalizedData, pojoStored);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      iterator.remove();
    }
  }

  private void validate(long windowId, List<JSONObject> finalizedData, Map<Long, List<POJO>> pojoStored) throws NoSuchFieldException, IllegalAccessException, JSONException
  {
    List<POJO> toBeAggregated = new LinkedList<>();

    long thisAndBeforeWindowId = windowId - (long)(this.activeAggregationInterval / this.windowTimeSec);
    Iterator<Map.Entry<Long, List<POJO>>> iterator = pojoStored.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, List<POJO>> next = iterator.next();
      if (next.getKey() <= thisAndBeforeWindowId) {
        toBeAggregated.addAll(next.getValue());
        iterator.remove();
      }
    }

    // Compute aggregates
    Map<AggregateKey, AggregateValue<POJO>> map = computeAggregates(toBeAggregated);

    // Iterate over finalizedData received and verify it.
    for (JSONObject jo : finalizedData) {
      // Get group keys
      JSONObject groupByKey = (JSONObject)jo.get("groupByKey");
      // Get aggregates
      JSONObject aggregates = (JSONObject)jo.get("aggregates");
      // Generate AggregateKey from groupByKey for lookup into the aggregate map.
      AggregateKey key = generateKey(groupByKey);
      // Verify actual and expected.
      verifyOrCrash(map, key, aggregates);
    }
  }

  private void verifyOrCrash(Map<AggregateKey, AggregateValue<POJO>> map, AggregateKey key, JSONObject aggregates) throws JSONException
  {
    // Receive AggregateValue holding all the aggreagates for given AggregateKey.
    AggregateValue<POJO> pojoAggregateValue = map.get(key);

    // Iterate over every value computation.
    for (Map.Entry<String, List<String>> entry : this.computations.entrySet()) {
      // fieldValue is the field on which aggregate will be calculated.
      String fieldValue = entry.getKey();
      // List of aggregate functions.
      List<String> aggFuncList = entry.getValue();
      // Get JSONObject for given fieldValue from actual result.
      JSONObject joForFieldValue = (JSONObject)aggregates.get(fieldValue);
      // Iterate over every aggregate function configured.
      for (String func : aggFuncList) {
        // Get expected Aggregate value.
        Number expectedAggregatedValue = (Number)getAggregate(pojoAggregateValue, fieldValue, func);
        // Get actual value from JSONObject.
        Number actualAggregatedValue = (Number)joForFieldValue.get(func);

        if (expectedAggregatedValue.doubleValue() != actualAggregatedValue.doubleValue()) {
          logger.error("TEST FAILED:: Key: {}, value: {}, aggFunc: {}, Actual : {}, Expected: {}", key, fieldValue, func, actualAggregatedValue, expectedAggregatedValue);
          String result = "TEST FAILED:: Key: " + key + ", value: " + fieldValue + ", aggFunc: " + func +
            ", Actual : " + actualAggregatedValue + ", Expected: " + expectedAggregatedValue;
          writeResult(result);
        }
      }
    }
  }

  private AggregateKey generateKey(JSONObject groupByKey) throws NoSuchFieldException, JSONException, IllegalAccessException
  {
    POJO pojo = new POJO();
    List<String> combination = new ArrayList<>();

    Iterator keysIterator = groupByKey.keys();
    while (keysIterator.hasNext()) {
      String key = (String)keysIterator.next();
      if (!key.equals("time")) {
        // Set in POJO
        Field field = pojo.getClass().getDeclaredField(key);
        field.setAccessible(true);
        field.set(pojo, groupByKey.get(key));

        // Add to combination key
        combination.add(key);
      }
    }
    return new AggregateKey(combination, pojo);
  }

  private Object getAggregate(AggregateValue<POJO> value, String field, String aggFunc)
  {
    return value.getAggregateValue(Aggregator.getAggregator(aggMapping.get(aggFunc) + "(" + field + ")"));
  }

  private void writeResult(String result)
  {
    try {
      out.writeBytes(result);
      out.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Map<AggregateKey, AggregateValue<POJO>> computeAggregates(List<POJO> toBeAggregated) throws NoSuchFieldException, IllegalAccessException
  {
    Map<AggregateKey, AggregateValue<POJO>> map = new HashMap<>();
    for (List<String> combination : combinations) {
      List<AggregateValue<POJO>> aggregateValues = Aggregations.groupBy(toBeAggregated, combination, allAggList);
      for (AggregateValue<POJO> aggregateValue : aggregateValues) {
        AggregateKey k = new AggregateKey(combination, aggregateValue.getObject());
        map.put(k, aggregateValue);
      }
    }

    return map;
  }

  private void appendToStatusFile(String content)
  {

  }

  public String getGroupByKeys()
  {
    return groupByKeys;
  }

  public void setGroupByKeys(String groupByKeys)
  {
    this.groupByKeys = groupByKeys;
  }

  public String getComputeFields()
  {
    return computeFields;
  }

  public void setComputeFields(String computeFields)
  {
    this.computeFields = computeFields;
  }

  public String getTimeBuckets()
  {
    return timeBuckets;
  }

  public void setTimeBuckets(String timeBuckets)
  {
    this.timeBuckets = timeBuckets;
  }

  public int getActiveAggregationInterval()
  {
    return activeAggregationInterval;
  }

  public void setActiveAggregationInterval(int activeAggregationInterval)
  {
    this.activeAggregationInterval = activeAggregationInterval;
  }

  public boolean isPrintFinalizedData()
  {
    return printFinalizedData;
  }

  public void setPrintFinalizedData(boolean printFinalizedData)
  {
    this.printFinalizedData = printFinalizedData;
  }
}
