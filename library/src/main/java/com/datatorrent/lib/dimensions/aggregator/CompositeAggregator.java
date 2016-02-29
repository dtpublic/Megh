package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;
import java.util.Set;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;

public interface CompositeAggregator
{
  public int getSchemaID();
  public int getDimensionDescriptorID();
  public int getAggregatorID();
  
  public Set<Integer> getEmbedAggregatorDdIds();
  
  public Set<String> getFields();
  public FieldsDescriptor getAggregateDescriptor();
  
  public FieldsDescriptor getMetaDataDescriptor();
  /**
   * Returns the output type of the {@link CompositeAggregator}. <b>Note<b> that any combination of input types
   * will produce the same output type for {@link CompositeAggregator}s.
   * @return The output type of the {@link CompositeAggregator}.
   */
  public Type getOutputType();
  
  /**
   * 
   * @param resultAggregate the aggregate to put the result
   * @param inputEventKeys The input(incremental) event keys, used to locate the input aggregates
   * @param inputAggregatesRepo: the map of the EventKey to Aggregate keep the super set of aggregate required
   */
  public void aggregate(Aggregate resultAggregate, Set<EventKey> inputEventKeys, Map<EventKey, Aggregate> inputAggregatesRepo);
}
