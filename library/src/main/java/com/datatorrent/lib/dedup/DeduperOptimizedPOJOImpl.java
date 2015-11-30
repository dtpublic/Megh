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
import com.datatorrent.lib.bucket.BasicBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.POJOBucketManager;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * A basic implementation of Deduper which uses a Basic Bucket Manager ({@link BasicBucketManagerPOJOImpl}). This
 * implementation takes a POJO as an input.
 * 
 */
public class DeduperOptimizedPOJOImpl extends AbstractDeduperOptimized<Object, Object>
{
  private transient Getter<Object, Object> getter;

  @Override
  public void setup(OperatorContext context)
  {
    ((HdfsBucketStore<Object>)bucketManager.getBucketStore()).setConfiguration(context.getId(),
        context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    super.setup(context);
  }

  @Override
  public void processTuple(Object event)
  {
    if (getter == null) {
      Class<?> fqcn = event.getClass();
      getter =
          PojoUtils.createGetter(fqcn, ((BasicBucketManagerPOJOImpl)bucketManager).getKeyExpression(), Object.class);
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

  /**
   * Returns the value of the dedup key to be used for the current tuple
   */
  @Override
  protected Object getEventKey(Object event)
  {
    return getter.get(event);
  }

  private static final transient Logger logger = LoggerFactory.getLogger(DeduperOptimizedPOJOImpl.class);
}
