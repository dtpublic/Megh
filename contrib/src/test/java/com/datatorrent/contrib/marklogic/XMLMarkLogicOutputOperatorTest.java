/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.marklogic;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;

/**
 * Functional test for {@link XMLMarkLogicOutputOperator}
 * This requires MarkLogic server to run locally as there is no mock server available for MarkLogic
 */
@Ignore
public class XMLMarkLogicOutputOperatorTest
{
  @Rule
  public TestInfo testInfo = new TestInfo();

  @Test
  public void test() throws Exception
  {
    String input = "<name>test</name>";
    String input1 = "<name>test1</name>";
    testInfo.connector.beginWindow(1L);
    testInfo.connector.input.process(input);
    testInfo.connector.endWindow();
    testInfo.connector.beginWindow(2L);
    testInfo.connector.input.process(input1);
    testInfo.connector.endWindow();
    testInfo.connector.beginWindow(3L);
    testInfo.connector.endWindow();
    testInfo.connector.committed(1L);
    testInfo.connector.committed(2L);
    Thread.sleep(2000);
    DatabaseClient databaseClient = DatabaseClientFactory.newClient(testInfo.hostName, testInfo.port, null,
      testInfo.userName, testInfo.password, DatabaseClientFactory.Authentication.valueOfUncased(testInfo.authType));
    XMLDocumentManager xmlDocumentManager = databaseClient.newXMLDocumentManager();
    QueryManager queryManager = databaseClient.newQueryManager();
    StringQueryDefinition queryDefinition = queryManager.newStringDefinition();
    queryDefinition.setDirectory(testInfo.documentDir);
    SearchHandle resultsHandle = queryManager.search(queryDefinition, new SearchHandle());
    MatchDocumentSummary[] documentSummary = resultsHandle.getMatchResults();
    Assert.assertEquals("Expected two documents", 2, documentSummary.length);
    boolean firstDocMatch = false;
    boolean secondDocMatch = false;
    String expectedResult;
    for (int i = 0; i < documentSummary.length; i++) {
      expectedResult = xmlDocumentManager.read(documentSummary[i].getUri(), new StringHandle().withFormat(Format.XML)).get();
      if (input.equals(expectedResult)) {
        firstDocMatch = true;
      } else if (input1.equals(expectedResult)) {
        secondDocMatch = true;
      }
      xmlDocumentManager.delete(documentSummary[i].getUri());
    }
    databaseClient.release();
    Assert.assertTrue("Matched both documents", firstDocMatch && secondDocMatch);
  }

  public static class TestInfo extends TestWatcher
  {
    XMLMarkLogicOutputOperator connector;
    String hostName = "localhost";
    String password = "admin";
    String userName = "admin";
    String authType = "digest";
    String documentDir = "/test/dir/";
    int port = 8000;

    @Override
    protected void starting(Description description)
    {
      connector = new XMLMarkLogicOutputOperator();
      connector.setHostName(hostName);
      connector.setPassword(password);
      connector.setUserName(userName);
      connector.setPort(port);
      connector.setDocumentDirectory(documentDir);
      connector.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      connector.teardown();
    }
  }
}
