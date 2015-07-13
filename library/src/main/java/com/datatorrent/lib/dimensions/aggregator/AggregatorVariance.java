package com.datatorrent.lib.dimensions.aggregator;

import java.util.List;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * This is the variance {@link OTFAggregator}.
 */
public class AggregatorVariance implements OTFAggregator
{
  private static final long serialVersionUID = 201507100202L;

  /**
   * The array index of the sum aggregates in the argument list of the {@link #aggregate} function.
   */
  public static int SUM_INDEX = 0;
  /**
   * The array index of the count aggregates in the argument list of the {@link #aggregate} function.
   */
  public static int COUNT_INDEX = 1;
  /**
   * The array index of the sum of square aggregates in the argument list of the {@link #aggregate} function.
   */
  public static int SUM_SQ_INDEX = 2;

  /**
   * The singleton instance of this class.
   */
  public static final AggregatorVariance INSTANCE = new AggregatorVariance();

  /**
   * The list of {@link IncrementalAggregator}s that this {@link OTFAggregator} depends on.
   */
  public static final transient List<Class<? extends IncrementalAggregator>> CHILD_AGGREGATORS = ImmutableList.of(AggregatorIncrementalType.SUM.getAggregator().getClass(), AggregatorIncrementalType.COUNT.getAggregator().getClass(), AggregatorIncrementalType.SQ_SUM.getAggregator().getClass());

  /**
   * Constructor for singleton pattern.
   */
  public AggregatorVariance()
  {
    // Do nothing.
  }

  @Override
  public List<Class<? extends IncrementalAggregator>> getChildAggregators()
  {
    return CHILD_AGGREGATORS;
  }

  @Override
  public GPOMutable aggregate(GPOMutable... aggregates)
  {
    Preconditions.checkArgument(aggregates.length == getChildAggregators().size(), "The number of arguments " + aggregates.length + " should be the same as the number of child aggregators " + getChildAggregators().size());

    GPOMutable sumAggregation = aggregates[SUM_INDEX];
    GPOMutable countAggregation = aggregates[COUNT_INDEX];
    GPOMutable sumSqAggregation = aggregates[SUM_SQ_INDEX];

    FieldsDescriptor fieldsDescriptor = sumAggregation.getFieldDescriptor();
    Fields fields = fieldsDescriptor.getFields();
    GPOMutable result = new GPOMutable(AggregatorUtils.getOutputFieldsDescriptor(fields, this));

    for (String field : fields.getFields()) {
      Type type = sumAggregation.getFieldDescriptor().getType(field);

      double sum = 0;
      double sumSq = 0;
      double count = (double) countAggregation.getFieldsLong()[0];

      switch (type) {
      case BYTE: {
        sum = (double) sumAggregation.getFieldByte(field);
        sumSq = (double) sumSqAggregation.getFieldByte(field);
        break;
      }
      case SHORT: {
        sum = (double) sumAggregation.getFieldShort(field);
        sumSq = (double) sumSqAggregation.getFieldShort(field);
        break;
      }
      case INTEGER: {
        sum = (double) sumAggregation.getFieldInt(field);
        sumSq = (double) sumSqAggregation.getFieldInt(field);
        break;
      }
      case LONG: {
        sum = (double) sumAggregation.getFieldLong(field);
        sumSq = (double) sumSqAggregation.getFieldLong(field);
        break;
      }
      case FLOAT: {
        sum = sumAggregation.getFieldFloat(field);
        sumSq = sumSqAggregation.getFieldFloat(field);
        break;
      }
      case DOUBLE: {
        sum = sumAggregation.getFieldDouble(field);
        sumSq = sumSqAggregation.getFieldDouble(field);
        break;
      }
      default: {
        throw new UnsupportedOperationException("The type " + type + " is not supported.");
      }
      }

      double val = (sumSq - (sum * sum / count)) / count;
      result.setField(field, val);
    }

    return result;
  }

  @Override
  public Type getOutputType()
  {
    return Type.DOUBLE;
  }

}
