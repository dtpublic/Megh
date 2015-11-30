/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Dimensional schema descriptor, this class is helper class to read dimensional event specification directly into java
 * objects which can be handled easily in java.
 *
 * Also this can be used to convert schema compatible with dimension computation event schema in json format.
 */
public class DimensionalSchemaDesc
{
  public static final String ONE_MINUTE = "1m";
  public static final String ONE_HOUR = "1h";
  public static final String ONE_DAY = "1d";
  public static final Combinations emptyCombination = new Combinations();

  public List<KeyDesc> keys = new ArrayList<KeyDesc>();
  public List<String> timeBuckets = new ArrayList<String>();
  public List<ValueDesc> values = new ArrayList<ValueDesc>();
  public List<Combinations> dimensions = new ArrayList<Combinations>();

  public DimensionalSchemaDesc()
  {
    dimensions.add(emptyCombination);
  }

  public static class KeyDesc
  {
    public String name;
    public String type;
    public List<String> enumValues = null;
  }

  public static class ValueDesc
  {
    public String name;
    public String type;
    public List<String> aggregators = new ArrayList<String>();
  }

  public static class Combinations
  {
    public List<String> combination = new ArrayList<String>();
  }
}
