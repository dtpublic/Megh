package com.datatorrent.malhar.lib.io.db;

/**
 * This interface is for a store that supports transactions
 *
 * @since 0.9.3
 */
public interface TransactionableStore extends Transactionable, Connectable
{
  /**
   * Gets the committed window id from a persistent store.
   *
   * @param appId      application id
   * @param operatorId operator id
   * @return the committed window id
   */
  public long getCommittedWindowId(String appId, int operatorId);

  /**
   * Stores the committed window id to a persistent store.
   *
   * @param appId      application id
   * @param operatorId operator id
   * @param windowId   window id
   */
  public void storeCommittedWindowId(String appId, int operatorId, long windowId);

  /**
   * Removes the committed window id from a persistent store.
   *
   * @param appId
   * @param operatorId
   */
  public void removeCommittedWindowId(String appId, int operatorId);
}
