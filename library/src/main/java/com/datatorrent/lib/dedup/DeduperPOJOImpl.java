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

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.POJOBucketManager;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerPOJOImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * An implementation of AbstractDeduper which takes in a POJO.
 * @displayName Deduper
 * @category Stream Manipulators
 * @tags dedup, pojo
 *
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class DeduperPOJOImpl extends AbstractDeduper<Object, Object>
{
  private transient Getter<Object, Object> getter;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> duplicates = new DefaultOutputPort<>();

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> expired = new DefaultOutputPort<>();

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    Preconditions.checkArgument(bucketManager.getBucketStore() instanceof ExpirableHdfsBucketStore);
    ExpirableHdfsBucketStore<Object> store = (ExpirableHdfsBucketStore<Object>)bucketManager.getBucketStore();
    store.setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    super.setup(context);
  }

  @Override
  public void activate(Context context)
  {
    super.activate(context);
    Preconditions.checkArgument(getPojoClass() != null);
    getter = PojoUtils.createGetter(getPojoClass(),
            ((TimeBasedBucketManagerPOJOImpl)bucketManager).getKeyExpression(), Object.class);
  }

  @Override
  protected Object convert(Object event)
  {
    return event;
  }

  /**
   * Sets the bucket manager implementation for POJO.
   *
   * @param bucketManager {@link BucketManager} to be used by deduper.
   */
  public void setBucketManager(@NotNull POJOBucketManager<Object> bucketManager)
  {
    this.bucketManager = Preconditions.checkNotNull(bucketManager, "storage manager");
    super.setBucketManager(bucketManager);
  }

  /**
   * The bucket manager implementation for POJO.
   *
   * @return Bucket Manager implementation for POJO.
   */
  @Override
  public POJOBucketManager<Object> getBucketManager()
  {
    return (POJOBucketManager<Object>)bucketManager;
  }

  @Override
  protected Object getEventKey(Object event)
  {
    return getter.get(event);
  }

  @Override
  protected void emitOutput(Object event)
  {
    output.emit(event);
  }

  @Override
  protected void emitDuplicate(Object event)
  {
    duplicates.emit(event);
  }

  @Override
  protected void emitExpired(Object event)
  {
    expired.emit(event);
  }

  @Override
  protected void emitError(Object event)
  {
    error.emit(event);
  }

  protected StreamCodec<Object> getDeduperStreamCodec()
  {
    return new DeduperStreamCodec(((TimeBasedBucketManagerPOJOImpl)bucketManager).getKeyExpression());
  }

  private static final transient Logger logger = LoggerFactory.getLogger(DeduperPOJOImpl.class);
}
