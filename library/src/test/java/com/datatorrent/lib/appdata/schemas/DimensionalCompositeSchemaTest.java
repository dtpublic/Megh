/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.dimensions.aggregator.AbstractTopBottomAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorBottom;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorTop;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.OTFAggregator;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import com.datatorrent.lib.appdata.gpo.Serde;
import com.datatorrent.lib.appdata.gpo.SerdeMapPrimitive;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class DimensionalCompositeSchemaTest
{
  protected final String onlyOneCompositeAggregatorSchema = 
      "{\"keys\":[{\"name\":\"publisher\",\"type\":\"string\"}, " +
      "{\"name\":\"location\",\"type\":\"string\"}], " +
      "\"timeBuckets\":[\"1m\"], " +
      "\"values\": " +
      " [{\"name\":\"impressions\",\"type\":\"long\",\"aggregators\":[ " +
      "      {\"aggregator\":\"TOPN\",\"count\":\"10\",\"embededAggregator\":\"SUM\",\"subCombinations\":" +
      "[\"location\"]}]}, " +
      " ], " +
      "\"dimensions\":" +
      " [{\"combination\":[\"publisher\"]}] " +
      "}";
  
  @Test
  public void testOneCompositeSchema() throws Exception
  {
    final String compositeAggregatorName = "TOPN-SUM-10_location";
    final DimensionalConfigurationSchema dimensionConfigSchema =
        new DimensionalConfigurationSchema(onlyOneCompositeAggregatorSchema,
        AggregatorRegistry.newDefaultAggregatorRegistry());
    final DimensionalSchema dimensional = new DimensionalSchema(dimensionConfigSchema);
    
    Map<String, IncrementalAggregator> incrementalNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToIncrementalAggregator();
    Map<String, AbstractTopBottomAggregator> compsiteNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToTopBottomAggregator();
    Assert.assertTrue(compsiteNameToAggregator.size() == 1 && compsiteNameToAggregator.keySet().iterator().next()
        .equals(compositeAggregatorName));
    
    Map<String, Integer> incremantalAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID();
    Map<String, Integer> compositeAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getTopBottomAggregatorNameToID();
    Assert.assertTrue(compositeAggregatorNameToID.size() == 1 && compositeAggregatorNameToID.keySet().iterator()
        .next().equals(compositeAggregatorName));
    Assert.assertTrue(compositeAggregatorNameToID.values().iterator().next() == incremantalAggregatorNameToID.size());
    
    //check output map
    List<Int2ObjectMap<FieldsDescriptor>> ddIdToAggregatorIDToOutputAggregatorDescriptor =
        dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor();
    
    List<Map<String, FieldsDescriptor>> ddIdToCompositeAggregatorToDescriptor =
        dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor();
    
    //there should have 2 ddId, 0(publisher) should only have composite aggregator;
    // 1(publisher, location) should have aggregator SUM
    {
      Int2ObjectMap<FieldsDescriptor> topAggregatorToDescriptor =
          ddIdToAggregatorIDToOutputAggregatorDescriptor.get(0);
      Assert.assertTrue(topAggregatorToDescriptor.keySet().size() == 1);
      Assert.assertTrue(topAggregatorToDescriptor.keySet().iterator().next() == dimensional.getAggregatorRegistry()
          .getTopBottomAggregatorNameToID().get(compositeAggregatorName));
    }
    
    {
      Int2ObjectMap<FieldsDescriptor> sumAggregatorToDescriptor = ddIdToAggregatorIDToOutputAggregatorDescriptor.get(1);
      Assert.assertTrue(sumAggregatorToDescriptor.keySet().size() == 1);
      Assert.assertTrue(sumAggregatorToDescriptor.keySet().iterator().next() ==
          dimensionConfigSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get("SUM"));
    }
  }
  
  protected final String multiCompositeAggregatorSchema =
      "{\"keys\":[{\"name\":\"publisher\",\"type\":\"string\"}," +
      "{\"name\":\"advertiser\",\"type\":\"string\"}," +
      "{\"name\":\"location\",\"type\":\"string\"}]," +
      "\"timeBuckets\":[\"1m\"]," +
      "\"values\":" +
      "[{\"name\":\"impressions\",\"type\":\"long\",\"aggregators\":[ " +
      "{\"aggregator\":\"TOPN\",\"count\":\"10\",\"embededAggregator\":\"SUM\",\"subCombinations\":" +
      "[\"location\"]}]}," +
      "{\"name\":\"cost\",\"type\":\"double\",\"aggregators\":[ " +
      "{\"aggregator\":\"TOPN\",\"count\":\"10\",\"embededAggregator\":\"SUM\",\"subCombinations\":[\"location\"]}," +
      "{\"aggregator\":\"BOTTOMN\",\"count\":\"20\",\"embededAggregator\":\"AVG\",\"subCombinations\":" +
      "[\"location\"]}]}]," +
      "\"dimensions\":[" +
      "{\"combination\":[\"publisher\"]}]}";

  @Test
  public void testMultiCompositeSchema() throws Exception
  {
    final String[] compositeAggregatorNames = {"TOPN-SUM-10_location", "BOTTOMN-AVG-20_location"};
    final DimensionalConfigurationSchema dimensionConfigSchema =
        new DimensionalConfigurationSchema(multiCompositeAggregatorSchema,
        AggregatorRegistry.newDefaultAggregatorRegistry());
    final DimensionalSchema dimensional = new DimensionalSchema(dimensionConfigSchema);
    
    Map<String, IncrementalAggregator> incrementalNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToIncrementalAggregator();
    Map<String, AbstractTopBottomAggregator> compsiteNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToTopBottomAggregator();
    //check composite aggregate names
    Assert.assertTrue(compsiteNameToAggregator.size() == 2 && compsiteNameToAggregator.keySet().equals(
        Sets.newHashSet(compositeAggregatorNames)));
    
    Map<String, Integer> incremantalAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID();
    Map<String, Integer> compositeAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getTopBottomAggregatorNameToID();
    
    //check output map
    List<Int2ObjectMap<FieldsDescriptor>> ddIdToAggregatorIDToOutputAggregatorDescriptor =
        dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor();
    
    List<Map<String, FieldsDescriptor>> ddIdToCompositeAggregatorToDescriptor =
        dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor();
    
    //there should have 2 ddId, 0(publisher) should only have composite aggregator;
    // 1(publisher, location) should have aggregator SUM, COUNT
    {
      Int2ObjectMap<FieldsDescriptor> topAggregatorToDescriptor = ddIdToAggregatorIDToOutputAggregatorDescriptor.get(0);
      Assert.assertTrue(topAggregatorToDescriptor.keySet().size() == 2);
      Set<Integer> compositeAggregatorIDs = Sets.newHashSet();
      for (String compositeAggregatorName : compositeAggregatorNames) {
        compositeAggregatorIDs.add(dimensional.getAggregatorRegistry().getTopBottomAggregatorNameToID()
            .get(compositeAggregatorName));
      }
      Assert.assertTrue(topAggregatorToDescriptor.keySet().equals(compositeAggregatorIDs));
    }
    
    {
      Int2ObjectMap<FieldsDescriptor> sumAggregatorToDescriptor = ddIdToAggregatorIDToOutputAggregatorDescriptor.get(1);
      
      //check key: aggregator ID
      Assert.assertTrue(sumAggregatorToDescriptor.keySet().size() == 2);
      Set<Integer> incrementalAggregatorIDs = Sets.newHashSet();
      incrementalAggregatorIDs.add(dimensionConfigSchema.getAggregatorRegistry()
          .getIncrementalAggregatorNameToID().get("SUM"));
      incrementalAggregatorIDs.add(dimensionConfigSchema.getAggregatorRegistry()
          .getIncrementalAggregatorNameToID().get("COUNT"));
      Assert.assertTrue(sumAggregatorToDescriptor.keySet().equals(incrementalAggregatorIDs));
      
      //check value: 
      {
        //SUM: impression and cost
        FieldsDescriptor fd = sumAggregatorToDescriptor.get(dimensionConfigSchema.getAggregatorRegistry()
            .getIncrementalAggregatorNameToID().get("SUM"));
        Map<String, Type> fieldToType = fd.getFieldToType();
        Assert.assertTrue(fieldToType.size() == 2);
        Assert.assertTrue(fieldToType.get("impressions").equals(Type.LONG));
        Assert.assertTrue(fieldToType.get("cost").equals(Type.DOUBLE));
      }
      {
        //COUNT: cost only
        FieldsDescriptor fd = sumAggregatorToDescriptor.get(dimensionConfigSchema.getAggregatorRegistry()
            .getIncrementalAggregatorNameToID().get("COUNT"));
        Map<String, Type> fieldToType = fd.getFieldToType();
        Assert.assertTrue(fieldToType.size() == 1);
        Assert.assertTrue(fieldToType.get("cost").equals(Type.LONG));
      }
    }
    
  }
  
  
  /**
   * test the schema of aggregator with embed schema and property
   * @throws Exception
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSchemaComposite() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaComposite.json");
    final DimensionalConfigurationSchema dimensionConfigSchema =
        new DimensionalConfigurationSchema(eventSchemaJSON, AggregatorRegistry.newDefaultAggregatorRegistry());
    final DimensionalSchema dimensional = new DimensionalSchema(dimensionConfigSchema);

    Map<String, AbstractTopBottomAggregator> compsiteNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToTopBottomAggregator();
    Map<String, IncrementalAggregator> incrementalNameToAggregator =
        dimensional.getAggregatorRegistry().getNameToIncrementalAggregator();
    Map<String, OTFAggregator> otfNameToAggregator = dimensional.getAggregatorRegistry().getNameToOTFAggregators();
    
    //verify the name to aggregator
    //expected name to aggregator
    Map<String, AbstractTopBottomAggregator> expectedNameToAggregator = Maps.newHashMap();
    final String[] combination_location = new String[]{"location"};
    final String[] combination_advertiser = new String[]{"advertiser"};
    final String[] combination_location_publisher = new String[]{"location", "publisher"};
    expectedNameToAggregator.put("TOPN-SUM-10_location",
        new AggregatorTop().withCount(10).withEmbedAggregatorName("SUM").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-AVG-20_location",
        new AggregatorBottom().withCount(20).withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
    expectedNameToAggregator.put("TOPN-SUM-10_location_publisher",
        new AggregatorTop().withCount(10).withEmbedAggregatorName("SUM")
        .withSubCombinations(combination_location_publisher));
    expectedNameToAggregator.put("TOPN-SUM-10_advertiser",
        new AggregatorTop().withCount(10).withEmbedAggregatorName("SUM").withSubCombinations(combination_advertiser));
    expectedNameToAggregator.put("TOPN-COUNT-10_location",
        new AggregatorTop().withCount(10).withEmbedAggregatorName("COUNT").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-AVG-10_location",
        new AggregatorBottom().withCount(10).withEmbedAggregatorName("AVG").withSubCombinations(combination_location));
    expectedNameToAggregator.put("BOTTOMN-SUM-10_location",
        new AggregatorBottom().withCount(10).withEmbedAggregatorName("SUM").withSubCombinations(combination_location));
    
    MapDifference difference = Maps.difference(compsiteNameToAggregator, expectedNameToAggregator);
    Assert.assertTrue("Generated Composit Aggregators are not same as expected.\n" + difference.toString(),
        difference.areEqual());
    
    
    //verify AggregateNameToFD
    //implicit added combinations
    Set<Set<String>> implicitAddedCombinations = Sets.newHashSet();
    {
      implicitAddedCombinations.add(Sets.newHashSet("location", "advertiser"));
      implicitAddedCombinations.add(Sets.newHashSet("location", "publiser"));
      implicitAddedCombinations.add(Sets.newHashSet("location", "advertiser", "publiser"));
    }
    
    //
    List<Map<String, FieldsDescriptor>> ddIDToAggregatorToDesc =
        dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor();
    List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggregatorIDToInputDesc =
        dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor();
    List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggregatorIDToOutputDesc =
        dimensionConfigSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor();
    List<IntArrayList> ddIDToIncrementalAggregatorIDs =
        dimensionConfigSchema.getDimensionsDescriptorIDToIncrementalAggregatorIDs();
    List<IntArrayList> ddIDToCompositeAggregatorIDs =
        dimensionConfigSchema.getDimensionsDescriptorIDToCompositeAggregatorIDs();
    
    
    //size
    final int expectedDdIDNum = (3 + implicitAddedCombinations.size()) * 2;
    Assert.assertTrue(ddIDToAggregatorToDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToAggregatorIDToInputDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToAggregatorIDToOutputDesc.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToIncrementalAggregatorIDs.size() == expectedDdIDNum);
    Assert.assertTrue(ddIDToCompositeAggregatorIDs.size() == expectedDdIDNum);
    
    //expectedAggregateNameToFD
    Map<String, FieldsDescriptor> expectedCommonAggregateNameToFD = Maps.newHashMap();
    
    final Type topBottomType = Type.OBJECT;
    
    //this object can be shared.
    Map<String, Serde> fieldToSerde = Maps.newHashMap();
    //common
    {
      //TOPN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", topBottomType);
      fieldToType.put("clicks", topBottomType);
      fieldToSerde.put("impressions", SerdeMapPrimitive.INSTANCE);
      fieldToSerde.put("clicks", SerdeMapPrimitive.INSTANCE);
      expectedCommonAggregateNameToFD.put("TOPN-SUM-10_location", new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    {
      //BOTTOMN-AVG-20_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("clicks", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("clicks", SerdeMapPrimitive.INSTANCE);
      expectedCommonAggregateNameToFD.put("BOTTOMN-AVG-20_location", new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    
    //specific
    //first
    Map<String, FieldsDescriptor> expectedFirstCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_location_publisher
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("impressions", SerdeMapPrimitive.INSTANCE);
      expectedFirstCombinationAggregateNameToFD.put("TOPN-SUM-10_location_publisher",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    
    //second
    Map<String, FieldsDescriptor> expectedSecondCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_advertiser
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("impressions", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("impressions", SerdeMapPrimitive.INSTANCE);
      expectedSecondCombinationAggregateNameToFD.put("TOPN-SUM-10_advertiser",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    
    Map<String, FieldsDescriptor> expectedThirdCombinationAggregateNameToFD = Maps.newHashMap();
    {
      //TOPN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("cost", SerdeMapPrimitive.INSTANCE);
      expectedThirdCombinationAggregateNameToFD.put("TOPN-SUM-10_location",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    {
      //TOPN-COUNT-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("cost", SerdeMapPrimitive.INSTANCE);
      expectedThirdCombinationAggregateNameToFD.put("TOPN-COUNT-10_location",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    {
      //BOTTOMN-SUM-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("cost", SerdeMapPrimitive.INSTANCE);
      expectedThirdCombinationAggregateNameToFD.put("BOTTOMN-SUM-10_location",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
    {
      //BOTTOMN-AVG-10_location
      Map<String, Type> fieldToType = Maps.newHashMap();
      fieldToType.put("cost", topBottomType);
      fieldToSerde.clear();
      fieldToSerde.put("cost", SerdeMapPrimitive.INSTANCE);
      expectedThirdCombinationAggregateNameToFD.put("BOTTOMN-AVG-10_location",
          new FieldsDescriptor(fieldToType, fieldToSerde));
    }
   
    // put common
    Map<String, FieldsDescriptor> commonAggregateNameToFD = Maps.newHashMap();
    commonAggregateNameToFD.put("TOPN-SUM-10_location", expectedCommonAggregateNameToFD.get("TOPN-SUM-10_location"));
    commonAggregateNameToFD.put("BOTTOMN-AVG-20_location",
        expectedCommonAggregateNameToFD.get("BOTTOMN-AVG-20_location"));
    
    List<Map<String, FieldsDescriptor>> expectedDdIDToAggregatorToDesc = Lists.newArrayListWithCapacity(8);
    {
      //first/second
      merge(expectedFirstCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(0, expectedFirstCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(1, expectedFirstCombinationAggregateNameToFD);
    }
    
    {
      //third/forth
      merge(expectedSecondCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(2, expectedSecondCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(3, expectedSecondCombinationAggregateNameToFD);
    }
        
    {
      //fifth/sixth
      merge(expectedThirdCombinationAggregateNameToFD, commonAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(4, expectedThirdCombinationAggregateNameToFD);
      expectedDdIDToAggregatorToDesc.add(5, expectedThirdCombinationAggregateNameToFD);
    }
    
    //others are empty
    for (int i = 6; i < expectedDdIDNum; ++i) {
      expectedDdIDToAggregatorToDesc.add(i, Collections.<String, FieldsDescriptor>emptyMap());
    }
    
    for (int index = 0; index < expectedDdIDNum; ++index) {
      MapDifference<String, FieldsDescriptor> diff =
          Maps.difference(expectedDdIDToAggregatorToDesc.get(index), ddIDToAggregatorToDesc.get(index));
      Assert.assertTrue(diff.toString(), diff.areEqual());
    }

    // verify aggregatorIDs
    Map<String, Integer> incremantalAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID();
    Map<String, Integer> compositeAggregatorNameToID =
        dimensionConfigSchema.getAggregatorRegistry().getTopBottomAggregatorNameToID();
    
    //common
    Set<Integer> expectedCommonAggregatorIds = Sets.newHashSet();
    expectedCommonAggregatorIds.add(incremantalAggregatorNameToID.get("SUM"));
    expectedCommonAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location"));
    expectedCommonAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-AVG-20_location"));
    
    //advertiser
    Set<Integer> expectedFirstCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedFirstCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedFirstCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location_publisher"));
    }
    
    //publisher
    Set<Integer> expectedSecondCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedSecondCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedSecondCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_advertiser"));
    }
    
    //"advertiser","publisher"
    Set<Integer> expectedThirdCombinationAggregatorIds = Sets.newHashSet();
    {
      expectedThirdCombinationAggregatorIds.addAll(expectedCommonAggregatorIds);
      expectedThirdCombinationAggregatorIds.add(incremantalAggregatorNameToID.get("MIN"));
      expectedThirdCombinationAggregatorIds.add(incremantalAggregatorNameToID.get("MAX"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-SUM-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("TOPN-COUNT-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-SUM-10_location"));
      expectedThirdCombinationAggregatorIds.add(compositeAggregatorNameToID.get("BOTTOMN-AVG-10_location"));
    }
        
    Set<Integer>[] expectedAllAggregatorIDSetArray = new Set[expectedDdIDNum];
    expectedAllAggregatorIDSetArray[0] = expectedAllAggregatorIDSetArray[1] = expectedFirstCombinationAggregatorIds;
    expectedAllAggregatorIDSetArray[2] = expectedAllAggregatorIDSetArray[3] = expectedSecondCombinationAggregatorIds;
    expectedAllAggregatorIDSetArray[4] = expectedAllAggregatorIDSetArray[5] = expectedThirdCombinationAggregatorIds;
    
    
    //implicit added dimension and incremental aggregators
    //there three combination of auto generated dimension
    
    Map<Set<String>, Set<Integer>> autoGeneratedKeysToAggregatorIds = Maps.newHashMap();
    final int sumId = incremantalAggregatorNameToID.get("SUM");
    final int countId = incremantalAggregatorNameToID.get("COUNT");
    
    //the two common composite TOP_SUM_10-location and BOTTOM_AVG_20-location 
    // depend combination {"advertiser", "location"}, {"location", "publisher"},
    // {"location", "advertiser", "publisher"} with aggregator {"SUM", "COUNT"}
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("advertiser", "location"), Sets.newHashSet(sumId, countId));
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("publisher", "location"), Sets.newHashSet(sumId, countId));
    autoGeneratedKeysToAggregatorIds.put(Sets.newHashSet("location", "advertiser", "publisher"),
        Sets.newHashSet(sumId, countId));
    
    //the specific composite aggregator of first combination depends combination
    // {"location", "advertiser", "publisher"} with aggregator {"SUM"}
    //it is already added by common composite aggregator

    //the "advertiser", "publisher" is existed combination.
    //the specific composite aggregator of second combination depends combination
    // {"advertiser", "publisher"} with aggregator {"SUM"}
    
    //the specific composite aggregator of third combination depends combination
    // {"location", "advertiser", "publisher"} with aggregator {"SUM", "COUNT"}
    
    //map from aggregator id set to the count of this aggregator id set
    Map<Set<Integer>, Integer> expectedAutoGeneratedIdsToCount = Maps.newHashMap();
    for (Set<Integer> generatorIds : autoGeneratedKeysToAggregatorIds.values()) {
      Integer count = expectedAutoGeneratedIdsToCount.get(generatorIds);
      if (count == null) {
        expectedAutoGeneratedIdsToCount.put(generatorIds, 2);   //2 is the size of timebucket
      } else {
        expectedAutoGeneratedIdsToCount.put(generatorIds, count + 2);
      }
    }
  
    //we can't guarantee the order of automatic generated aggregators, Compare with map instead of list
    Map<Set<Integer>, Integer> autoGeneratedIdsToCount = Maps.newHashMap();
    for (int index = 0; index < expectedDdIDNum; ++index) {
      IntArrayList incrementalAggregatorIDs = ddIDToIncrementalAggregatorIDs.get(index);
      Set<Integer> incrementalAggregatorIDSet = Sets.newHashSet(incrementalAggregatorIDs.toArray(new Integer[0]));
      Assert.assertTrue("There are duplicate aggregator IDs.",
          incrementalAggregatorIDs.size() == incrementalAggregatorIDSet.size());
      
      IntArrayList compositeAggregatorIDs = ddIDToCompositeAggregatorIDs.get(index);
      Set<Integer> compositeAggregatorIDSet = Sets.newHashSet(compositeAggregatorIDs.toArray(new Integer[0]));
      Assert.assertTrue("There are duplicate aggregator IDs.",
          compositeAggregatorIDs.size() == compositeAggregatorIDSet.size());
      
      Set<Integer> allAggregatorIDSet = Sets.newHashSet();
      allAggregatorIDSet.addAll(incrementalAggregatorIDSet);
      allAggregatorIDSet.addAll(compositeAggregatorIDSet);
      Assert.assertTrue("There are overlap aggregator IDs.",
          allAggregatorIDSet.size() == incrementalAggregatorIDSet.size() + compositeAggregatorIDSet.size());
      
      if (index < 6) {
        SetView<Integer> diff1 = Sets.difference(allAggregatorIDSet, expectedAllAggregatorIDSetArray[index]);
        SetView<Integer> diff2 = Sets.difference(expectedAllAggregatorIDSetArray[index], allAggregatorIDSet);
     
        Assert.assertTrue("Not Same aggregator ids. ddID: " + index + "; details: \n" + diff1 + "; " + diff2, 
            diff1.isEmpty() && diff2.isEmpty() );
      } else {
        Integer count = autoGeneratedIdsToCount.get(allAggregatorIDSet);
        if (count == null) {
          autoGeneratedIdsToCount.put(allAggregatorIDSet, 1);
        } else {
          autoGeneratedIdsToCount.put(allAggregatorIDSet, count + 1);
        }
      }
    }
    
    MapDifference<Set<Integer>, Integer> diff =
        Maps.difference(autoGeneratedIdsToCount, expectedAutoGeneratedIdsToCount);
    Assert.assertTrue(diff.toString(), diff.areEqual());
  }
  
  

  /**
   * merge right into left and return left
   * @param left
   * @param right
   * @return
   */
  protected Map<String, FieldsDescriptor> merge(Map<String, FieldsDescriptor> left, 
      Map<String, FieldsDescriptor> right)
  {
    for (Map.Entry<String, FieldsDescriptor> rightEntry : right.entrySet()) {
      String rightKey = rightEntry.getKey();
      if (left.get(rightKey) == null) {
        left.put(rightKey, rightEntry.getValue());
      } else {
        FieldsDescriptor leftFd = left.get(rightKey);
        Map<String, Type> leftFieldToType = leftFd.getFieldToType();
        Map<String, Type> rightFieldToType = rightEntry.getValue().getFieldToType();
        leftFieldToType.putAll(rightFieldToType);
        leftFd = new FieldsDescriptor(leftFieldToType);
        left.put(rightKey, leftFd);
      }
    }
    return left;
  }
}
