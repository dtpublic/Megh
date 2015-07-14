package com.datatorrent.malhar.lib.io.db;

/**
 * This interface is for any service that provides transactional feature
 *
 * @since 0.9.3
 */
public interface Transactionable
{
  /**
   * Begins a transaction.
   */
  public void beginTransaction();

  /**
   * Commits the current transaction.
   */
  public void commitTransaction();

  /**
   * Rolls back the current transaction.
   */
  public void rollbackTransaction();

  /**
   * @return returns whether currently is in a transaction.
   */
  public boolean isInTransaction();
}
