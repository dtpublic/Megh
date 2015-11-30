/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStoreAsync;
import com.datatorrent.lib.bucket.ExpirableOrderedBucketManagerPOJOImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * An implementation of Deduper which uses an Ordered Bucket Manager ({@link ExpirableOrderedBucketManagerPOJOImpl}).
 * This implementation takes POJO as input.
 *
 * @displayName Deduper
 * @tags dedup, pojo
 */
public class DeduperOrderedPOJOImpl extends AbstractDeduperOptimized<Object, Object>
{
  private transient Getter<Object, Object> getter;

  @Override
  public void setup(OperatorContext context)
  {
    ((ExpirableHdfsBucketStoreAsync<Object>)bucketManager.getBucketStore()).setConfiguration(context.getId(),
        context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    super.setup(context);
  }

  @Override
  public void processTuple(Object event)
  {
    if (getter == null) {
      Class<?> fqcn = event.getClass();
      getter =
          PojoUtils.createGetter(fqcn, ((ExpirableOrderedBucketManagerPOJOImpl)bucketManager).getKeyExpression(),
              Object.class);
    }

    super.processTuple(event);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object convert(Object event)
  {
    return event;
  }

  /**
   * Sets the bucket manager implementation for POJO.
   *
   * @param bucketManager
   *          {@link BucketManager} to be used by deduper.
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

  /**
   * Returns the value of the dedup key to be used for the current tuple
   */
  @Override
  protected Object getEventKey(Object event)
  {
    return getter.get(event);
  }

  private static final transient Logger logger = LoggerFactory.getLogger(DeduperOrderedPOJOImpl.class);
}
