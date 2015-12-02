/*
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.contrib.marklogic;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Lists;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.fs.AbstractReconciler;

/**
 * MarkLogicOutputOperator is the base class to write data to MarkLogic Database.<br/>
 * To have a better performance, the data in each window is broken into batches based on the batch size and
 * each batch is asynchronously written to MarkLogic Database
 * <p/>
 * Properties that be set on MarkLogicOutputOperator are:<br/>
 * {@link #userName}: The user with read, write, or administrative privileges<br/>
 * {@link #password}: The password for the user<br/>
 * {@link #dbName}: The database to access<br/>
 * {@link #authType}: The type of authentication applied to the request<br/>
 * {@link #hostName}: The host with the REST server <br/>
 * {@link #port}: The port for the REST server<br/>
 * {@link #batchSize}: The batch size of batch<br/>
 *
 * @param <T> input type
 */

public abstract class MarkLogicOutputOperator<T> extends AbstractReconciler<T, List<T>>
{
  /**
   * Database client for making database requests
   */
  protected transient DatabaseClient databaseClient;
  /**
   * The user with read, write, or administrative privileges
   */
  protected String userName;
  /**
   * The password for the user
   */
  protected String password;
  /**
   * The database to access
   */
  protected String dbName;
  /**
   * The type of authentication applied to the request
   */
  protected String authType = "digest";
  /**
   * The host with the REST server
   */
  @NotNull
  protected String hostName;
  /**
   * The port for the REST server
   */
  protected int port = 8000;
  /**
   * The batch size of batch
   */
  private int batchSize = 1000;
  private transient List<T> batch = null;
  private transient int currentBatchSize = 0;
  protected transient DatabaseClientFactory.Authentication authentication;

  /**
   * This returns client to access the MarkLogic Database by means of a REST server
   *
   * @return A new client for making database requests
   */
  public abstract DatabaseClient initializeClient();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (authType != null) {
      authentication = DatabaseClientFactory.Authentication.valueOfUncased(authType);
    }
    databaseClient = initializeClient();
  }

  @Override
  protected void processTuple(T input)
  {
    currentBatchSize++;
    batch.add(input);

    if (currentBatchSize >= batchSize) {
      enqueueForProcessing(batch);
      batch = Lists.newLinkedList();
      currentBatchSize = 0;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    batch = Lists.newLinkedList();
    currentBatchSize = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (!batch.isEmpty()) {
      enqueueForProcessing(batch);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    databaseClient.release();
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public String getAuthType()
  {
    return authType;
  }

  public void setAuthType(String authType)
  {
    this.authType = authType;
  }

}
