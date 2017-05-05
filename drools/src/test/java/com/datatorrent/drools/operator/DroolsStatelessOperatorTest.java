/**
 * Copyright (c) 2017 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.drools.operator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.kie.api.definition.KiePackage;
import org.kie.api.runtime.StatelessKieSession;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.drools.rules.Product;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class DroolsStatelessOperatorTest
{
  CollectorTestSink<Object> factsPort = new CollectorTestSink<Object>();
  CollectorTestSink<Object> factsAndFiredRulesPort = new CollectorTestSink<>();
  CollectorTestSink<Object> ruleCountPort = new CollectorTestSink<>();

  public class TestMeta extends TestWatcher
  {
    private String rulesDirectory;
    private String goldRuleFileName = "rulesForGold.drl";
    private String diamondRuleFileName = "rulesForDiamond.drl";
    private String xlsRulesFileName = "ShopRules.xls";
    private DroolsStatelessOperator underTest;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.rulesDirectory = "target/" + className + "/" + methodName + "/rules";

      try {
        createRulesFiles();
      } catch (IOException e) {
        e.printStackTrace();
      }
      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH,
          "target/" + className + "/" + methodName + "/" + Long.toHexString(System.currentTimeMillis()));
      context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes);
      underTest = new DroolsStatelessOperator();
      underTest.setRulesDir(rulesDirectory);
      underTest.setup(context);
      underTest.factsOutput.setSink(factsPort);
      underTest.factAndFiredRulesOutput.setSink(factsAndFiredRulesPort);
      underTest.ruleCountOutput.setSink(ruleCountPort);
    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName()));
      factsAndFiredRulesPort.clear();
      ruleCountPort.clear();
      factsPort.clear();
    }

    private void createRulesFiles() throws IOException
    {
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource("rules/" + diamondRuleFileName).getFile());
      File rulesFile = new File(rulesDirectory, diamondRuleFileName);
      FileUtils.copyFile(file, rulesFile);
      file = new File(classLoader.getResource("rules/" + goldRuleFileName).getFile());
      rulesFile = new File(rulesDirectory, goldRuleFileName);
      FileUtils.copyFile(file, rulesFile);
      file = new File(classLoader.getResource("rules/" + xlsRulesFileName).getFile());
      rulesFile = new File(rulesDirectory, xlsRulesFileName);
      FileUtils.copyFile(file, rulesFile);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testActivation()
  {
    testMeta.underTest.activate(testMeta.context);
    StatelessKieSession session = testMeta.underTest.getKieSession();
    Collection<KiePackage> packages = session.getKieBase().getKiePackages();
//    System.out.println(packages);
  }

  @Test
  public void testSpringActivation()
  {
    testMeta.underTest.setLoadSpringSession(true);
    testMeta.underTest.setSessionName("test-session");
    testMeta.underTest.activate(testMeta.context);
    StatelessKieSession session = testMeta.underTest.getKieSession();
    Collection<KiePackage> packages = session.getKieBase().getKiePackages();

//    for (KiePackage kiePackage : packages) {
//      System.out.println(kiePackage);
//      for (org.kie.api.definition.rule.Rule rule : kiePackage.getRules()) {
//        System.out.println(rule.getName());
//      }
//    }

  }

  protected List<Object> getListForRuleMatch()
  {
    List<Object> factList = new ArrayList<>();
    factList.add(new Product(1,"gold",0));
    factList.add(new Product(2,"gold",0));
    factList.add(new Product(3,"gold",0));
    factList.add(new Product(4,"gold",0));

    factList.add(new Product(3,"diamond",0));
    factList.add(new Product(3,"diamond",0));
    factList.add(new Product(3,"diamond",0));

    return factList;
  }

  protected List<Object> getListForNoRuleMatch()
  {
    List<Object> factList = new ArrayList<>();
    factList.add(new Product(1,"fakeGold",0));
    factList.add(new Product(21,"fakeGold",0));
    factList.add(new Product(3,"fakeGold",0));
    factList.add(new Product(4,"fakeGold",0));

    factList.add(new Product(3,"fakeDiamond",0));
    factList.add(new Product(3,"fakeDiamond",0));
    factList.add(new Product(3,"fakeDiamond",0));

    return factList;
  }

  @Test
  public void testListenerRulesMatch()
  {
    testMeta.underTest.setLoadSpringSession(true);
    testMeta.underTest.setSessionName("test-session");
    testMeta.underTest.activate(testMeta.context);
    Assert.assertEquals(0,factsAndFiredRulesPort.collectedTuples.size());
    Assert.assertEquals(0,factsPort.collectedTuples.size());
    Assert.assertEquals(0,ruleCountPort.collectedTuples.size());
    List<Object> factsList = getListForRuleMatch();
    testMeta.underTest.beginWindow(0);
    for (Object fact : factsList) {
      testMeta.underTest.factsInput.process(fact);
    }
    Assert.assertEquals(0,factsPort.collectedTuples.size());
    testMeta.underTest.endWindow();
    Assert.assertEquals(1,ruleCountPort.collectedTuples.size());
    Assert.assertEquals(1,factsAndFiredRulesPort.collectedTuples.size());
    //It is expected to get 8 tuples as platinum is added via rules
    Assert.assertEquals(8,factsPort.collectedTuples.size());
    Map<Object, List<org.kie.api.definition.rule.Rule>> factsAndFiredRules =
      (Map<Object, List<org.kie.api.definition.rule.Rule>>)factsAndFiredRulesPort.collectedTuples.get(0);
    Assert.assertEquals(8,factsAndFiredRules.size());
    Map<org.kie.api.definition.rule.Rule, Integer> ruleCount =
      (Map<org.kie.api.definition.rule.Rule, Integer>)ruleCountPort.collectedTuples.get(0);
    Assert.assertEquals(3,ruleCount.size());
  }

  @Test
  public void testListenerNoRulesMatch()
  {
    testMeta.underTest.setLoadSpringSession(true);
    testMeta.underTest.setSessionName("test-session");
    testMeta.underTest.activate(testMeta.context);
    Assert.assertEquals(0,factsAndFiredRulesPort.collectedTuples.size());
    Assert.assertEquals(0,factsPort.collectedTuples.size());
    Assert.assertEquals(0,ruleCountPort.collectedTuples.size());
    List<Object> factsList = getListForNoRuleMatch();
    testMeta.underTest.beginWindow(0);
    for (Object fact : factsList) {
      testMeta.underTest.factsInput.process(fact);
    }
    testMeta.underTest.endWindow();
    Assert.assertEquals(1,factsAndFiredRulesPort.collectedTuples.size());
    Assert.assertEquals(7,factsPort.collectedTuples.size());
    Assert.assertEquals(1,ruleCountPort.collectedTuples.size());
    Map<Object, List<org.kie.api.definition.rule.Rule>> factsAndFiredRules =
      (Map<Object, List<org.kie.api.definition.rule.Rule>>)factsAndFiredRulesPort.collectedTuples.get(0);
    Assert.assertEquals(7,factsAndFiredRules.size());
    Map<org.kie.api.definition.rule.Rule, Integer> ruleCount =
      (Map<org.kie.api.definition.rule.Rule, Integer>)ruleCountPort.collectedTuples.get(0);
    Assert.assertEquals(0,ruleCount.size());
  }

  @Test
  public void testListenerRulesMatchBatch()
  {
    testMeta.underTest.setLoadSpringSession(true);
    testMeta.underTest.setSessionName("test-session");
    testMeta.underTest.activate(testMeta.context);
    Assert.assertEquals(0,factsAndFiredRulesPort.collectedTuples.size());
    Assert.assertEquals(0,factsPort.collectedTuples.size());
    Assert.assertEquals(0,ruleCountPort.collectedTuples.size());
    List<Object> factsList = getListForRuleMatch();
    testMeta.underTest.setBatchSize(5);
    testMeta.underTest.beginWindow(0);
    for (Object fact : factsList) {
      testMeta.underTest.factsInput.process(fact);
    }
    Assert.assertEquals(6,factsPort.collectedTuples.size());
    testMeta.underTest.endWindow();
    Assert.assertEquals(1,ruleCountPort.collectedTuples.size());
    Assert.assertEquals(1,factsAndFiredRulesPort.collectedTuples.size());
    //It is expected to get 8 tuples as platinum is added via rules
    Assert.assertEquals(8,factsPort.collectedTuples.size());
    Map<Object, List<org.kie.api.definition.rule.Rule>> factsAndFiredRules =
      (Map<Object, List<org.kie.api.definition.rule.Rule>>)factsAndFiredRulesPort.collectedTuples.get(0);
    Assert.assertEquals(8,factsAndFiredRules.size());
    Map<org.kie.api.definition.rule.Rule, Integer> ruleCount =
      (Map<org.kie.api.definition.rule.Rule, Integer>)ruleCountPort.collectedTuples.get(0);
    Assert.assertEquals(3,ruleCount.size());
  }


}
