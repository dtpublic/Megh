/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.dimensions;

/**
 * This class used to identify an aggregation.
 * kind like EventKey except don't include key values
 *
 *
 * @since 3.4.0
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
