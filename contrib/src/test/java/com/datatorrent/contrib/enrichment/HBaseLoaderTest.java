/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.enrichment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.netlet.util.DTThrowable;

public class HBaseLoaderTest
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(HBaseLoaderTest.class);

  public static class TestMeta extends TestWatcher
  {

    HBaseLoader dbloader;
    @Override
    protected void starting(Description description)
    {
      try {
        dbloader = new HBaseLoader();
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("file:///home/chaitanya/hbase-site.xml"));

        dbloader.setConfiguration(conf);
        dbloader.setZookeeperQuorum("localhost");
        dbloader.setZookeeperClientPort(2181);

        dbloader.setTableName("EMPLOYEE");

        dbloader.connect();
        createTable();
        insertRecordsInTable();
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }
    }

    private void createTable()
    {
      try {
        String[] familys = {"personal", "professional"};
        HBaseAdmin admin = new HBaseAdmin(dbloader.getConfiguration());
        HTableDescriptor tableDesc = new HTableDescriptor(dbloader.getTableName());
        for (int i = 0; i < familys.length; i++) {
          tableDesc.addFamily(new HColumnDescriptor(familys[i]));
        }
        admin.createTable(tableDesc);

        logger.debug("Table  created successfully...");
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }
    }

    @SuppressWarnings("deprecation")
    public void addRecord(String rowKey, String family, String qualifier, String value) throws Exception
    {
      try {
        HTable table = new HTable(dbloader.getConfiguration(), dbloader.getTableName());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
            .toBytes(value));
        table.put(put);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }

    private void insertRecordsInTable()
    {
      try {
        addRecord("row1", "personal", "name", "raju");
        addRecord("row1", "personal", "city", "hyderabad");
        addRecord("row1", "professional", "designation", "manager");
        addRecord("row1", "professional", "Salary", "50000");

        addRecord("row2", "personal", "name", "ravi");
        addRecord("row2", "personal", "city", "Chennai");
        addRecord("row2", "professional", "designation", "SE");
        addRecord("row2", "professional", "Salary", "30000");

        addRecord("row3", "personal", "name", "rajesh");
        addRecord("row3", "personal", "city", "Delhi");
        addRecord("row3", "professional", "designation", "E");
        addRecord("row3", "professional", "Salary", "10000");
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }

    }

    private void cleanTable()
    {
      String sql = "delete from  " + dbloader.getTableName();
      try {
        HBaseAdmin admin = new HBaseAdmin(dbloader.getConfiguration());
        admin.disableTable(dbloader.getTableName());
        admin.deleteTable(dbloader.getTableName());
      } catch (MasterNotRunningException e) {
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected void finished(Description description)
    {
      cleanTable();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testHBaseLookup() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<String> includeKeys = new ArrayList<String>();
    includeKeys.add("city");
    includeKeys.add("Salary");
    ArrayList<String> lookupKeys = new ArrayList<String>();
    lookupKeys.add("ID");
    testMeta.dbloader.setFields(lookupKeys, includeKeys);

    String includeFamilyStr = "personal, professional";
    testMeta.dbloader.setIncludeFamilyStr(includeFamilyStr);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("row2");

    ArrayList<Object> columnInfo = (ArrayList<Object>)testMeta.dbloader.get(keys);

    Assert.assertEquals("CITY", "Chennai", columnInfo.get(0).toString().trim());
    Assert.assertEquals("Salary", 30000, columnInfo.get(1));
  }
}
