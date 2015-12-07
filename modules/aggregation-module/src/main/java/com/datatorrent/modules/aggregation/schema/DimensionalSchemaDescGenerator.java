/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.schema;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.datatorrent.api.StringCodec;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

public class DimensionalSchemaDescGenerator
{
  private DimensionalSchemaDesc schema = new DimensionalSchemaDesc();

  public static final String TIME_FIELD_EXP = "System.currentTimeMillis()";

  public String generateSchema(String groupByKeys, String computeFields, String timeBuckets, String qualifiedPOJOPath) throws NoSuchFieldException, IOException
  {
    Set<String> keySet = new HashSet<>();

    StringCodec.Class2String class2String = new StringCodec.Class2String();
    Class clazz = class2String.fromString(qualifiedPOJOPath);

    for (String combinationStr : groupByKeys.split(";")) {
      DimensionalSchemaDesc.Combinations c = new DimensionalSchemaDesc.Combinations();
      for (String key : combinationStr.split(",")) {
        c.combination.add(key);
        keySet.add(key);
      }
      schema.dimensions.add(c);
    }

    for (String keyName : keySet) {
      DimensionalSchemaDesc.KeyDesc keyDesc = new DimensionalSchemaDesc.KeyDesc();
      keyDesc.name = keyName;
      Class<?> type = clazz.getDeclaredField(keyName).getType();
      keyDesc.type = TypeInfo.getNameFromType(type);
      schema.keys.add(keyDesc);
    }

    for (String valueStr : computeFields.split(";")) {
      String[] split = valueStr.split(":");
      if (split.length != 2) {
        throw new RuntimeException("Invalid parameter given : " + valueStr);
      } else {
        DimensionalSchemaDesc.ValueDesc valueDesc = new DimensionalSchemaDesc.ValueDesc();
        valueDesc.name = split[0];
        Class<?> type = clazz.getDeclaredField(split[0]).getType();
        valueDesc.type = TypeInfo.getNameFromType(type);
        for (String agg : split[1].split(",")) {
          valueDesc.aggregators.add(agg);
        }
        schema.values.add(valueDesc);
      }
    }

    for (String tb : timeBuckets.split(",")) {
      schema.timeBuckets.add(tb);
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
  }

  public void setSchema(String configurationJSON) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    ObjectReader reader = mapper.reader(DimensionalSchemaDesc.class);
    schema = reader.readValue(configurationJSON);
  }

  public Map<String, String> generateGetters(String timeFieldName, String qualifiedPOJOPath) throws NoSuchFieldException
  {
    Map<String, String> expressionMap = new HashMap<>();

    StringCodec.Class2String class2String = new StringCodec.Class2String();
    Class clazz = class2String.fromString(qualifiedPOJOPath);

    for (DimensionalSchemaDesc.KeyDesc key : schema.keys) {
      expressionMap.put(key.name, key.name);
    }

    for (DimensionalSchemaDesc.ValueDesc value : schema.values) {
      expressionMap.put(value.name, value.name);
    }

    Class<?> type = clazz.getDeclaredField(timeFieldName).getType();

    if (type == Date.class) {
      expressionMap.put(DimensionsDescriptor.DIMENSION_TIME, getGetterMethodName(timeFieldName) + ".getTime()");
    } else if (type == long.class || type == int.class) {
      expressionMap.put(DimensionsDescriptor.DIMENSION_TIME, timeFieldName);
    }
    return expressionMap;
  }

  private String getGetterMethodName(String fieldName)
  {
    return "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
  }
}
