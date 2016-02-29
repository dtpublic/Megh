/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Set;

import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema.DimensionsConversionContext;

/**
 * SimpleCompositAggregator is the aggregator which embed other aggregator
 *
 *
 */
public abstract class AbstractCompositeAggregator implements CompositeAggregator
{
  private static final long serialVersionUID = 661710563764433621L;

  protected String embedAggregatorName;
  protected int dimensionDescriptorID;
  protected int aggregatorID;
  protected FieldsDescriptor aggregateDescriptor;
  //protected int embedAggregatorID;
  protected Set<Integer> embedAggregatorDdIds = Sets.newHashSet();
  protected Set<String> fields = Sets.newHashSet();
  
  protected DimensionsConversionContext dimensionsConversionContext;
  
  public DimensionsConversionContext getDimensionsConversionContext()
  {
    return dimensionsConversionContext;
  }

  public void setDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.dimensionsConversionContext = dimensionsConversionContext;
  }

  public AbstractCompositeAggregator withDimensionsConversionContext(DimensionsConversionContext dimensionsConversionContext)
  {
    this.setDimensionsConversionContext(dimensionsConversionContext);
    return this;
  }
  
  public String getEmbedAggregatorName()
  {
    return embedAggregatorName;
  }

  public void setEmbedAggregatorName(String embedAggregatorName)
  {
    this.embedAggregatorName = embedAggregatorName;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dimensionsConversionContext == null) ? 0 : dimensionsConversionContext.hashCode());
//    result = prime * result + ((embedAggregator == null) ? 0 : embedAggregator.hashCode());
    result = prime * result + ((embedAggregatorName == null) ? 0 : embedAggregatorName.hashCode());
    return result;
  }


  @Override
  public int getDimensionDescriptorID()
  {
    return dimensionDescriptorID;
  }
  public void setDimensionDescriptorID(int dimensionDescriptorID)
  {
    this.dimensionDescriptorID = dimensionDescriptorID;
  }
  
  @Override
  public int getAggregatorID()
  {
    return aggregatorID;
  }
  public void setAggregatorID(int aggregatorID)
  {
    this.aggregatorID = aggregatorID;
  }

  @Override
  public FieldsDescriptor getAggregateDescriptor()
  {
    return aggregateDescriptor;
  }
  public void setAggregateDescriptor(FieldsDescriptor aggregateDescriptor)
  {
    this.aggregateDescriptor = aggregateDescriptor;
  }
  
  @Override
  public Set<String> getFields()
  {
    return fields;
  }

  public void setFields(Set<String> fields)
  {
    this.fields = fields;
  }

  @Override
  public int getSchemaID()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  //implement this, the ddid in fact should be a set or list. or return the first ddid, and use the timebucket to get other ddids.
  //or think about get rid of this method in this class and implement outside.
  //if the embeded aggregator is OTF, just keep the ddid of OTF as depended incremental aggregators should have same ddid
  @Override
  public Set<Integer> getEmbedAggregatorDdIds()
  {
    return embedAggregatorDdIds;
  }

  public void addEmbedAggregatorDdId(int ddid)
  {
    embedAggregatorDdIds.add(ddid);
  }
  public void addEmbedAggregatorDdIds(Set<Integer> ddids)
  {
    embedAggregatorDdIds.addAll(ddids);
  }
  
  /**
   * bright: TODO: check
   */
  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return null;
  }
  
}
