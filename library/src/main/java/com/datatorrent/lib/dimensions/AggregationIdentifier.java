package com.datatorrent.lib.dimensions;

/**
 * This class used to identify an aggregation. 
 * kind like EventKey except don't include key values
 *
 */
public class AggregationIdentifier
{
  protected int schemaID;
  protected int dimensionDescriptorID;
  protected int aggregatorID;
  
  protected AggregationIdentifier(){}
      
  public AggregationIdentifier(int schemaID,
      int dimensionDescriptorID,
      int aggregatorID)
  {
    this.setSchemaID(schemaID);
    this.setDimensionDescriptorID(dimensionDescriptorID);
    this.setAggregatorID(aggregatorID);
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    return ((schemaID * 0) + dimensionDescriptorID) * prime + aggregatorID;
  }


  /**
   * it seems the there had some problem with schemaID. 
   * When using one schema, it could be different, sometime zero, sometime 1.
   * Ignore compare schemaID now as in fact only support one schema.
   */
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AggregationIdentifier other = (AggregationIdentifier)obj;
    if (aggregatorID != other.aggregatorID) {
      return false;
    }
    if (dimensionDescriptorID != other.dimensionDescriptorID) {
      return false;
    }

    return true;
  }


  public int getSchemaID()
  {
    return schemaID;
  }

  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  public int getDimensionDescriptorID()
  {
    return dimensionDescriptorID;
  }

  public void setDimensionDescriptorID(int dimensionDescriptorID)
  {
    this.dimensionDescriptorID = dimensionDescriptorID;
  }

  public int getAggregatorID()
  {
    return aggregatorID;
  }

  public void setAggregatorID(int aggregatorID)
  {
    this.aggregatorID = aggregatorID;
  }
}
