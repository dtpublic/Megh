/*
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.contrib.marklogic;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.lang3.tuple.MutablePair;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.StringHandle;

/**
 * Functional test for {@link JSONMarkLogicOutputOperator}
 * This requires MarkLogic server to run locally as there is no mock server available for MarkLogic
 */
@Ignore
public class JSONMarkLogicOutputOperatorTest
{
  @Rule
  public TestInfo testInfo = new TestInfo();

  @Test
  public void test() throws Exception
  {
    String input = "{\"name\":\"Iced Mocha\", \"size\":\"Grandé\", \"tasty\":true}";
    String input1 = "{\"name\":\"Iced Mocha\", \"tasty\":true}";
    String docId = "/a/afternoon-drink.json";
    String docId1 = "/a/afternoon-drink1.json";
    testInfo.connector.beginWindow(1L);
    testInfo.connector.input.process(new MutablePair<>(docId, input));
    testInfo.connector.endWindow();
    testInfo.connector.beginWindow(2L);
    testInfo.connector.input.process(new MutablePair<>(docId1, input1));
    testInfo.connector.endWindow();
    testInfo.connector.beginWindow(3L);
    testInfo.connector.endWindow();
    testInfo.connector.committed(1L);
    testInfo.connector.committed(2L);
    Thread.sleep(2000);
    DatabaseClient databaseClient = DatabaseClientFactory.newClient(testInfo.hostName, testInfo.port, null,
      testInfo.userName, testInfo.password, DatabaseClientFactory.Authentication.valueOfUncased(testInfo.authType));
    JSONDocumentManager jsonDocumentManager = databaseClient.newJSONDocumentManager();
    String expectedResult = jsonDocumentManager.read(docId, new StringHandle()).get();
    Assert.assertEquals("Expected data", input, expectedResult);
    jsonDocumentManager.delete(docId);
    jsonDocumentManager.delete(docId1);
    databaseClient.release();
  }

  public static class TestInfo extends TestWatcher
  {
    JSONMarkLogicOutputOperator connector;
    String hostName = "localhost";
    String password = "admin";
    String userName = "admin";
    String authType = "digest";
    int port = 8000;

    @Override
    protected void starting(Description description)
    {
      connector = new JSONMarkLogicOutputOperator();
      connector.setHostName(hostName);
      connector.setPassword(password);
      connector.setUserName(userName);
      connector.setPort(port);
      connector.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      connector.teardown();
    }
  }
}
