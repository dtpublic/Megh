package com.datatorrent.modules.app.aggregation.regression;

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

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import net.sf.jagg.AggregateFunction;
import net.sf.jagg.Aggregations;
import net.sf.jagg.Aggregator;
import net.sf.jagg.model.AggregateValue;

public class Validator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(Validator.class);

  private static final Map<String, String> aggMapping;

  //  private static final Map<String, String> inverseAggMapping;
  static {
    aggMapping = new HashMap<>();
    aggMapping.put("SUM", "SumAggregator");
    aggMapping.put("COUNT", "CountAggregator");
    aggMapping.put("AVG", "AvgAggregator");
  }

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

  private long windowId = 0;

  private Map<Long, List<POJO>> minPojoStored = new LinkedHashMap<>();
  private Map<Long, List<String>> toBeProcessed = new LinkedHashMap<>();

  private String groupByKeys;
  private String computeFields;
  private String timeBuckets;
  private int activeAggregationInterval;

  private double windowTimeSec;

  private List<List<String>> combinations = new ArrayList<>();
  private Map<String, List<String>> computations = new HashMap<>();
  private List<AggregateFunction> allAggList = new ArrayList<>();
  private List<String> buckets = new ArrayList<>();

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
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  private void processModuleInputTuple(POJO tuple)
  {
    List<POJO> list;
    if (minPojoStored.containsKey(this.windowId)) {
      list = minPojoStored.get(this.windowId);
    } else {
      list = new LinkedList<>();
      minPojoStored.put(this.windowId, list);
    }
    tuple.setTimeBucket("1m");
    list.add(tuple);
  }

  private void processModuleOutputTuple(String tuple)
  {
    List<String> list;
    if (toBeProcessed.containsKey(this.windowId)) {
      list = toBeProcessed.get(this.windowId);
    } else {
      list = new LinkedList<>();
      toBeProcessed.put(this.windowId, list);
    }
    list.add(tuple);
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<Long, List<String>>> iterator = toBeProcessed.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, List<String>> next = iterator.next();
      long windowId = next.getKey();
      List<String> finalizedData = next.getValue();
      try {
        validate(windowId, finalizedData);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
      iterator.remove();
    }
  }

  private void validate(long windowId, List<String> finalizedData) throws NoSuchFieldException, IllegalAccessException, JSONException
  {
    List<POJO> toBeAggregated = new LinkedList<>();

    long thisAndBeforeWindowId = windowId - (long)(this.activeAggregationInterval / this.windowTimeSec);
    Iterator<Map.Entry<Long, List<POJO>>> iterator = minPojoStored.entrySet().iterator();
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
    for (String s : finalizedData) {
      // Parse received JSON Object.
      JSONObject jo = new JSONObject(s);
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
    //
    //
    //    List<AggregateValue<POJO>> aggValues = Aggregations.groupBy(toBeAggregated, groupByKeys, allAggList);
    //
    //    for (AggregateValue<POJO> aggValue : aggValues )
    //    {
    //      StringBuffer buf = new StringBuffer();
    //      for (String key : groupByKeys) {
    //        buf.append(" ")
    //           .append(key)
    //           .append("=")
    //           .append(aggValue.getPropertyValue(key));
    //      }
    //
    //      buf.append(":");
    //
    //      for (AggregateFunction aggregator : allAggList)
    //      {
    //        buf.append(" ");
    //        buf.append(aggregator.getProperty());
    //        buf.append("(");
    //        buf.append(inverseAggMapping.get(aggregator.getClass().getSimpleName()));
    //        buf.append(")");
    //        buf.append("=");
    //        buf.append(aggValue.getAggregateValue(aggregator));
    //      }
    //      logger.info("Agg: {}", buf.toString());
    //    }
    //
    //    return aggValues;
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
}
