package com.datatorrent.drools.operator;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.kie.api.definition.KiePackage;
import org.kie.api.runtime.StatelessKieSession;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class StatelessDroolsOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    private String rulesDirectory;
    private String goldRuleFileName = "rulesForGold.drl";
    private String diamondRuleFileName = "rulesForDiamond.drl";
    private String xlsRulesFileName = "ShopRules.xls";
    private StatelessDroolsOperator underTest;
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
      underTest = new StatelessDroolsOperator();
      underTest.setRulesDir(rulesDirectory);
    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName()));
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
}
