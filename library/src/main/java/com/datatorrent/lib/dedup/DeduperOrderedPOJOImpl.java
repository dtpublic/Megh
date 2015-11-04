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

import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.ExpirableOrderedBucketManagerPOJOImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of AbstractDeduper which takes in a POJO.
 * @displayName Deduper
 * @category Stream Manipulators
 * @tags dedup, pojo
 *
 * @since 2.1.0
 */
public class DeduperOrderedPOJOImpl extends AbstractDeduperOptimized<Object, Object>
{
  private transient Getter<Object, Object> getter;

  @Override
  public void setup(OperatorContext context)
  {
    ((ExpirableHdfsBucketStore<Object>) bucketManager.getBucketStore()).setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    super.setup(context);
  }

  @Override
  public void processTuple(Object event)
  {
    if (getter==null) {
      Class<?> fqcn = event.getClass();
      getter = PojoUtils.createGetter(fqcn, ((ExpirableOrderedBucketManagerPOJOImpl) bucketManager).getKeyExpression(), Object.class);
    }

    super.processTuple(event);
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
  public void setBucketManager(@NotNull BucketManager<Object> bucketManager)
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
  public BucketManager<Object> getBucketManager()
  {
    return (BucketManager<Object>)bucketManager;
  }

  @Override
  protected Object getEventKey(Object event)
  {
    return getter.get(event);
  }

  private static transient final Logger logger = LoggerFactory.getLogger(DeduperOrderedPOJOImpl.class);

}
