/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.NonOperationalBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerPOJOImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * An implementation of AbstractDeduper which takes in a POJO.
 * @displayName Deduper
 * @category Rules and Alerts
 * @tags dedup, pojo
 *
 * @since 2.1.0
 */
@InterfaceStability.Evolving
public class DeduperTimeBasedPOJOImpl extends AbstractDeduper<Object, Object>
{
  private transient Getter<Object, Object> getter;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final transient DefaultInputPort<Object> inputPojo = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      Class<?> fqcn = context.getValue(Context.PortContext.TUPLE_CLASS);
      getter = PojoUtils.createGetter(fqcn, ((TimeBasedBucketManagerPOJOImpl) bucketManager).getKeyExpression(), Object.class);
    }

    @Override
    public final void process(Object tuple)
    {
      processTuple(tuple);
    }
  };

  public DeduperTimeBasedPOJOImpl()
  {
    bucketManager = new TimeBasedBucketManagerPOJOImpl();
    bucketManager.setBucketStore(new ExpirableHdfsBucketStore<>());
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
    if (stateless) {
      bucketManager.setBucketStore(new NonOperationalBucketStore<>());
    } else {
      ((HdfsBucketStore<Object>) bucketManager.getBucketStore()).setConfiguration(context.getId(),
        context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    }
    super.setup(context);
  }

  @Override
  protected Object convert(Object event)
  {
    return event;
  }

  /**
   * Sets the time-based POJO bucket manager implementation.
   *
   * @return Bucket Manager implementation for POJO.
   * @description $.timeExpression field in the pojo that expresses the time.
   * @useSchema $.timeExpression inputPojo.fields[].name
   * @description $.keyExpression field in the pojo which is the key of an event.
   * @useSchema $.keyExpression inputPojo.fields[].name
   */
  public void setBucketManager(@NotNull TimeBasedBucketManagerPOJOImpl bucketManager)
  {
    this.bucketManager = Preconditions.checkNotNull(bucketManager, "storage manager");
    super.setBucketManager(bucketManager);
  }

  @Override
  public TimeBasedBucketManagerPOJOImpl getBucketManager()
  {
    return (TimeBasedBucketManagerPOJOImpl) bucketManager;
  }

  @Override
  protected Object getEventKey(Object event)
  {
    return getter.get(event);
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(DeduperTimeBasedPOJOImpl.class);

}
