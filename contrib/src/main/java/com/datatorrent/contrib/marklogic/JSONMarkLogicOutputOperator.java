/*
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.contrib.marklogic;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import com.datatorrent.api.Context;

/**
 * JSONMarkLogicOutputOperator writes the json documents to MarkLogic Database. <br/>
 * It takes in a pair of < String,String>, wherein the first string represents document URI in database
 * and second string is json data to be stored. <br/>
 */

public class JSONMarkLogicOutputOperator extends MarkLogicOutputOperator<MutablePair<String, String>>
{

  private transient JSONDocumentManager documentManager;
  private transient DocumentWriteSet batchSet;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    documentManager = databaseClient.newJSONDocumentManager();
    batchSet = documentManager.newWriteSet();
  }

  @Override
  public DatabaseClient initializeClient()
  {
    return DatabaseClientFactory.newClient(hostName, port, dbName, userName, password, authentication);
  }

  @Override
  protected void processCommittedData(List<MutablePair<String, String>> pairs)
  {
    for (MutablePair<String, String> pair : pairs) {
      batchSet.add(pair.getLeft(), new StringHandle(pair.getRight()).withFormat(Format.JSON));
    }
    documentManager.write(batchSet);
    batchSet.clear();
  }
}
