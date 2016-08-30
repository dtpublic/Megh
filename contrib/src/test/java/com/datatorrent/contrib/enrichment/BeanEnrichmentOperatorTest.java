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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class BeanEnrichmentOperatorTest extends JDBCLoaderTest
{
  public class Order
  {
    public int OID;
    public int ID;
    public double amount;

    public Order(int oid, int id, double amount)
    {
      this.OID = oid;
      this.ID = id;
      this.amount = amount;
    }

    public int getOID()
    {
      return OID;
    }

    public void setOID(int OID)
    {
      this.OID = OID;
    }

    public int getID()
    {
      return ID;
    }

    public void setID(int ID)
    {
      this.ID = ID;
    }

    public double getAmount()
    {
      return amount;
    }

    public void setAmount(double amount)
    {
      this.amount = amount;
    }
  }


  @Test
  public void includeSelectedKeys()
  {
    POJOEnrichmentOperator oper = new POJOEnrichmentOperator();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFieldsStr("ID");
    oper.setIncludeFieldsStr("NAME,AGE,ADDRESS");
    oper.outputClass = EmployeeOrder.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ",
        "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=0.0}",
        sink.collectedTuples.get(0).toString());
  }

  @Test
  public void includeAllKeys()
  {
    POJOEnrichmentOperator oper = new POJOEnrichmentOperator();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFieldsStr("ID");
    oper.outputClass = EmployeeOrder.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);


    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ",
        "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=65000.0}",
        sink.collectedTuples.get(0).toString());
  }
}

