/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.contrib.cassandra.CassandraOperatorTest.TestPojo;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.helper.TestPortContext;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.netlet.util.DTThrowable;

public class CassandraQueryOutputOperatorTest
{
  private static final String NODE = "localhost";
  private static final String KEYSPACE = "mykeyspace";
  private static final String TABLE_NAME = "test";
  private static final String APP_ID = "CassandraOperatorTest";
  private static final int OPERATOR_ID = 0;
  private static Cluster cluster = null;
  private static Session session = null;
  private static final String configJsonString = "[{\"input\": \"id\", \"dataType\": \"INTEGER\", \"dbColumnName\": \"id\"},"
      + "{\"input\": \"age\", \"dataType\": \"INTEGER\", \"dbColumnName\": \"age\"},"
      + "{\"input\": \"doubleValue\", \"dataType\": \"DOUBLE\", \"dbColumnName\": \"doubleValue\"},"
      + "{\"input\": \"floatValue\", \"dataType\": \"FLOAT\", \"dbColumnName\": \"floatValue\"},"
      + "{\"input\": \"last_visited\", \"dataType\": \"TIMESTAMP\", \"dbColumnName\": \"last_visited\"},"
      + "{\"input\": \"lastname\", \"dataType\": \"text\", \"dbColumnName\": \"lastname\"},"
      + "{\"input\": \"list1\", \"dataType\": \"list<int>\", \"dbColumnName\": \"list1\"},"
      + "{\"input\": \"map1\", \"dataType\": \"map<text,int>\", \"dbColumnName\": \"map1\"},"
      + "{\"input\": \"set1\", \"dataType\": \"set<int>\", \"dbColumnName\": \"set1\"},"
      + "{\"input\": \"test\", \"dataType\": \"boolean\", \"dbColumnName\": \"test\"}]";

  private OperatorContextTestHelper.TestIdOperatorContext context;
  private TestOutputOperator underTest;

  @SuppressWarnings("unused")
  private static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  @BeforeClass
  public static void setup()
  {
    @SuppressWarnings("UnusedDeclaration")
    Class<?> clazz = org.codehaus.janino.CompilerFactory.class;
    try {
      cluster = Cluster.builder().addContactPoint(NODE).build();
      session = cluster.connect(KEYSPACE);

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( "
          + CassandraTransactionalStore.DEFAULT_APP_ID_COL + " TEXT, "
          + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, "
          + CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, " + "PRIMARY KEY ("
          + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL
          + ") " + ");";
      session.execute(createMetaTable);
      String createTable = "CREATE TABLE IF NOT EXISTS "
          + KEYSPACE
          + "."
          + TABLE_NAME
          + " (id uuid PRIMARY KEY,age int,lastname text,test boolean,floatvalue float,doubleValue double,set1 set<int>,list1 list<int>,map1 map<text,int>,last_visited timestamp);";
      session.execute(createTable);
    } catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    if (session != null) {
      session.execute("DROP TABLE " + CassandraTransactionalStore.DEFAULT_META_TABLE);
      session.execute("DROP TABLE " + KEYSPACE + "." + TABLE_NAME);
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  @Before
  public void setupForTest()
  {
    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();
    transactionalStore.setNode(NODE);
    transactionalStore.setKeyspace(KEYSPACE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    underTest = new TestOutputOperator();
    underTest.setTablename(TABLE_NAME);
    underTest.setStore(transactionalStore);
    underTest.setConfigJsonString(configJsonString);
    underTest.setup(context);
  }

  @After
  public void afterTest()
  {
    session.execute("TRUNCATE " + CassandraTransactionalStore.DEFAULT_META_TABLE);
    session.execute("TRUNCATE " + KEYSPACE + "." + TABLE_NAME);
  }

  @Test
  public void testJsonFieldsParsing()
  {

    List<FieldInfo> fieldInfos = underTest.getFieldInfos();
    Assert.assertEquals("Mismatch in number of fields.", 10, fieldInfos.size());
  }

  @Test
  public void testupdateQueryWithParameters()
  {
    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPojo.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    underTest.input.setup(tpc);
    underTest.activate(context);

    UUID id = UUID.randomUUID();
    TestPojo testPojo = new TestPojo(id, 20, "Laura", true, 10, 2.0, new HashSet<Integer>(), new ArrayList<Integer>(),
        null, new Date(System.currentTimeMillis()));

    //insert record
    underTest.beginWindow(0);
    underTest.input.process(testPojo);
    underTest.endWindow();
    underTest.setFieldInfos(new ArrayList<FieldInfo>());
    Assert.assertEquals("rows in db", 1, underTest.getNumOfEventsInStore());

    //update record
    String updateLastName = "Laurel";
    String updateConfigJson = "[{\"input\": \"id\", \"dataType\": \"INTEGER\", \"dbColumnName\": \"id\"}]";
    String updateQuery = "update " + KEYSPACE + "." + TABLE_NAME + " set lastname='" + updateLastName + "' where id=?";

    //reset the operator to run new query
    underTest.setConfigJsonString(updateConfigJson);
    underTest.setQuery(updateQuery);
    underTest.setup(context);
    underTest.activate(context);

    underTest.beginWindow(1);
    underTest.input.process(testPojo);
    underTest.endWindow();

    String recordsQuery = "SELECT * from " + TABLE_NAME + ";";
    ResultSet resultSetRecords = session.execute(recordsQuery);
    for (Row row : resultSetRecords) {
      Assert.assertEquals("Updated last name", updateLastName, row.getString("lastname"));
    }
  }

  @Test
  public void testCassandraOutputOperator()
  {
    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPojo.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    underTest.input.setup(tpc);
    underTest.activate(context);

    List<TestPojo> events = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      Set<Integer> set = new HashSet<Integer>();
      set.add(i);
      List<Integer> list = new ArrayList<Integer>();
      list.add(i);
      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("key" + i, i);
      events.add(new TestPojo(UUID.randomUUID(), i, "abclast", true, i, 2.0, set, list, map, new Date(System
          .currentTimeMillis())));
    }

    underTest.beginWindow(0);
    for (TestPojo event : events) {
      underTest.input.process(event);
    }
    underTest.endWindow();

    Assert.assertEquals("rows in db", 3, underTest.getNumOfEventsInStore());
    underTest.getEventsInStore();
  }

  private static class TestOutputOperator extends CassandraQueryOutputOperator
  {
    public long getNumOfEventsInStore()
    {
      String countQuery = "SELECT count(*) from " + TABLE_NAME + ";";
      ResultSet resultSetCount = session.execute(countQuery);
      for (Row row : resultSetCount) {
        return row.getLong(0);
      }
      return 0;

    }

    public void getEventsInStore()
    {
      String recordsQuery = "SELECT * from " + TABLE_NAME + ";";
      ResultSet resultSetRecords = session.execute(recordsQuery);
      for (Row row : resultSetRecords) {
        int age = row.getInt("age");
        Assert.assertEquals("check boolean", true, row.getBool("test"));
        Assert.assertEquals("check last name", "abclast", row.getString("lastname"));
        Assert.assertEquals("check double", 2.0, row.getDouble("doubleValue"), 2);
        Assert.assertNotEquals("check date", new Date(System.currentTimeMillis()), row.getDate("last_visited"));
        if (age == 2) {
          Assert.assertEquals("check float", 2.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(2);
          list.add(2);
          map.put("key2", 2);

          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
        if (age == 0) {
          Assert.assertEquals("check float", 0.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(0);
          list.add(0);
          map.put("key0", 0);
          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
        if (age == 1) {
          Assert.assertEquals("check float", 1.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(1);
          list.add(1);
          map.put("key1", 1);
          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
      }
    }
  }
}
