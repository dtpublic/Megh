package com.datatorrent.contrib.dimensions;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;


public class DimensionQueryResultMergeUnifierTester
{
  public static class SimpleSink<T> implements Sink<T>
  {
    protected List<T> data = Lists.newArrayList();
    
    @Override
    public void put(T tuple)
    {
      data.add(tuple);
    }

    @Override
    public int getCount(boolean reset)
    {
      return data.size();
    }
    
  }
  
  public static final String nonEmptyTupleTemplate =
      "{\"id\":\"%s\",\"type\":\"dataResult\",\"data\":[{\"time\":\"1452188760000\",\"region\":\"94\",\"zipcode\":" +
      "\"94598\",\"wait:AVG\":\"32.0\",\"lon:FIRST\":\"-122.02618\",\"lat:FIRST\":\"37.91878\"},{\"time\":" +
      "\"1452189300000\",\"region\":\"93\",\"zipcode\":\"93648\",\"wait:AVG\":\"8.0\",\"lon:FIRST\":" +
      "\"-119.52873\",\"lat:FIRST\":\"36.61365\"}],\"countdown\":\"299\"}";
  public static final String emptyTupleTemplate =
      "{\"id\":\"%s\",\"type\":\"dataResult\",\"data\":[],\"countdown\":\"299\"}";
  
  protected Map<String, List<String>> idToTuplesMap = Maps.newHashMap();
  protected Map<String, String> expectedIdToTuple = Maps.newHashMap();
  
  @Before
  public void prepareData()
  {
    int key = 1;
    addAllEmpty( "" + key++);
    addEmptyAndNonEmpty( "" + key++);
    addAllEmpty( "" + key++);
    addEmptyAndNonEmpty( "" + key++);
    addEmptyAndNonEmpty( "" + key++);
    addAllEmpty( "" + key++);
    addAllEmpty( "" + key++);
  }
  
  protected void addAllEmpty(String key)
  {
    final String emptyTuple = String.format(emptyTupleTemplate, key, key);
    idToTuplesMap.put(key, Lists.<String>newArrayList(emptyTuple, emptyTuple, emptyTuple));
    
    expectedIdToTuple.put(key, emptyTuple);
  }
  
  protected void addEmptyAndNonEmpty(String key)
  {
    final String emptyTuple = String.format(emptyTupleTemplate, key, key);
    final String nonEmptyTuple = String.format(nonEmptyTupleTemplate, key, key);
    idToTuplesMap.put(key, Lists.<String>newArrayList(emptyTuple, emptyTuple, nonEmptyTuple, emptyTuple, emptyTuple));
    
    expectedIdToTuple.put(key, nonEmptyTuple);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void test()
  {
    DimensionQueryResultMergeUnifier unifier = new DimensionQueryResultMergeUnifier();
    DefaultOutputPort<String> output = unifier.output;
    SimpleSink<Object> simpleSink = new SimpleSink<Object>();
    output.setSink(simpleSink);
    
    unifier.beginWindow(1);
    for (List<String> tuples : idToTuplesMap.values()) {
      for (String tuple : tuples) {
        unifier.process(tuple);
      }
    }
    unifier.endWindow();
    
    //verify
    assertCollectionSame(simpleSink.data, (Collection)expectedIdToTuple.values());
  }
  
  protected void assertCollectionSame(Collection<Object> expected, Collection<Object> actual)
  {
    Assert.assertTrue("Not same size.", expected.size() == actual.size());
    expected.removeAll(actual);
    Assert.assertTrue("Data not same.", expected.size() == 0);
  }

}
