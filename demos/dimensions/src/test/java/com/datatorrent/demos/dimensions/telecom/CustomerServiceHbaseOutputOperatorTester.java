 package com.datatorrent.demos.dimensions.telecom;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerServiceHBaseConf;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceHbaseOutputOperator;

public class CustomerServiceHbaseOutputOperatorTester {
  @Before
  public void setUp()
  {
    CustomerServiceHBaseConf.instance().setHost("localhost");
  }
  
  @Test
  public void test() throws Exception
  {
    CustomerEnrichedInfoHBaseConfig.instance().setHost("localhost");
    TelecomDemoConf.instance.setCdrDir("target/CDR");
    
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);

    populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf) {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    
    Thread.sleep(600000);

    lc.shutdown();   
  }
  
  protected void populateDAG(DAG dag, Configuration conf)
  {
    CustomerServiceGenerateOperator customerServiceGenerator = new CustomerServiceGenerateOperator();
    dag.addOperator("CustomerService-Generator", customerServiceGenerator);
    
    CustomerServiceHbaseOutputOperator hbaseOutput = new CustomerServiceHbaseOutputOperator();
    dag.addOperator("CustomerService-Output", hbaseOutput);
    
    dag.addStream("CustomerService", customerServiceGenerator.outputPort, hbaseOutput.input);
  }
}
