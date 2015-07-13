package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * This {@link IncrementalAggregator} performs a sum of square over the fields in the given {@link InputEvent}.
 * This extends from {@link AggregatorSum} as most of the functionality except one method is common for this.
 */
public class AggregatorSqSum extends AggregatorSum
{
  private static final long serialVersionUID = 201507100202L;

  public AggregatorSqSum()
  {
    super();
  }


  public void aggregateInput(GPOMutable destAggs, GPOMutable srcAggs)
  {
    {
      byte[] destByte = destAggs.getFieldsByte();
      if(destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();
        int[] srcIndices = context.indexSubsetAggregates.fieldsByteIndexSubset;
        for(int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[srcIndices[index]] * srcByte[srcIndices[index]];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if(destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();
        int[] srcIndices = context.indexSubsetAggregates.fieldsShortIndexSubset;
        for(int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[srcIndices[index]] * srcShort[srcIndices[index]];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if(destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();
        int[] srcIndices = context.indexSubsetAggregates.fieldsIntegerIndexSubset;
        for(int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[srcIndices[index]] * srcInteger[srcIndices[index]];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if(destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();
        int[] srcIndices = context.indexSubsetAggregates.fieldsLongIndexSubset;
        for(int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[srcIndices[index]] * srcLong[srcIndices[index]];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if(destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();
        int[] srcIndices = context.indexSubsetAggregates.fieldsFloatIndexSubset;
        for(int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[srcIndices[index]] * srcFloat[srcIndices[index]];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if(destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();
        int[] srcIndices = context.indexSubsetAggregates.fieldsDoubleIndexSubset;
        for(int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[srcIndices[index]] * srcDouble[srcIndices[index]];
        }
      }
    }
  }


}
