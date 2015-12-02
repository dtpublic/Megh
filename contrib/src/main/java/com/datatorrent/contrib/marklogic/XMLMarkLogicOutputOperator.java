/*
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.contrib.marklogic;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentUriTemplate;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import com.datatorrent.api.Context;

/**
 * XMLMarkLogicOutputOperator writes the xml documents to MarkLogic Database. <br/>
 * It takes in xml data as a string and inserts that into MarkLogic Database. Server internally generates the URI for
 * the document inserted<br/>
 *
 * <p/>
 * Properties that be set are : <br/>
 * {@link #uriExtension}: The extension that should suffix the document uri.<br/>
 * {@link #documentDirectory}: The directory that should prefix the document uri.
 *
 */
public class XMLMarkLogicOutputOperator extends MarkLogicOutputOperator<String>
{
  private transient XMLDocumentManager documentManager;
  private transient DocumentUriTemplate documentUriTemplate;

  @NotNull
  private String uriExtension = ".txt";
  private String documentDirectory;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    documentManager = databaseClient.newXMLDocumentManager();
    documentUriTemplate = documentManager.newDocumentUriTemplate(uriExtension);
    if (documentDirectory != null) {
      documentUriTemplate.setDirectory(documentDirectory);
    }
  }

  @Override
  public DatabaseClient initializeClient()
  {
    return DatabaseClientFactory.newClient(hostName, port, dbName, userName, password, authentication);
  }

  @Override
  protected void processCommittedData(List<String> strings)
  {
    for (String str : strings) {
      documentManager.create(documentUriTemplate, new StringHandle(str).withFormat(Format.XML));
    }
  }

  public String getUriExtension()
  {
    return uriExtension;
  }

  public void setUriExtension(String uriExtension)
  {
    this.uriExtension = uriExtension;
  }

  public String getDocumentDirectory()
  {
    return documentDirectory;
  }

  public void setDocumentDirectory(String documentDirectory)
  {
    this.documentDirectory = documentDirectory;
  }
}
