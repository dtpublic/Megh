package com.datatorrent.lib.dimensions.aggregator;

import java.util.List;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

/**
 * This is the standard deviation {@link OTFAggregator}.
 */
public class AggregatorStdDeviation extends AggregatorVariance
{
  private static final long serialVersionUID = 201507100207L;

  /**
   * The singleton instance of this class.
   */
  public static final AggregatorStdDeviation INSTANCE = new AggregatorStdDeviation();

  /**
   * Constructor for singleton pattern.
   */
  public AggregatorStdDeviation()
  {
    // Do nothing.
  }

  @Override
  public List<Class<? extends IncrementalAggregator>> getChildAggregators()
  {
    return super.getChildAggregators();
  }

  @Override
  public GPOMutable aggregate(GPOMutable... aggregates)
  {
    GPOMutable aggregate = super.aggregate(aggregates);

    FieldsDescriptor fieldsDescriptor = aggregate.getFieldDescriptor();
    Fields fields = fieldsDescriptor.getFields();

    for (String field : fields.getFields()) {
      double val = Math.sqrt(aggregate.getFieldDouble(field));
      aggregate.setField(field, val);
    }

    return aggregate;
  }
}
