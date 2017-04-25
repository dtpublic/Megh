/**
 * Put your copyright and license info here.
 */
package com.datatorrent.drools.rules.DroolListenerTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.definition.rule.Rule;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

import com.datatorrent.drools.rules.DroolListener.RulesFiredListener;
import com.datatorrent.drools.rules.Product;

public class RulesFiredListenerTest
{

  private StatelessKieSession kieSession;
  Map<Object,List<Rule>> factRule;
  Map<Rule,Integer> ruleCount;
  protected void initialize()
  {
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer;
    kieContainer = kieServices.getKieClasspathContainer();
    kieSession = kieContainer.newStatelessKieSession("ksession-rule");
    factRule = new HashMap<>();
    ruleCount = new HashMap<>();
    AgendaEventListener listener=new RulesFiredListener(factRule, ruleCount);
    kieSession.addEventListener(listener);
  }


  @Test
  public void RulesFiredListenerTest() throws Exception {
    initialize();

    Product gold1 = new Product(1,"gold",0);
    Product gold2 = new Product(2,"gold",0);
    Product gold3 = new Product(3,"gold",0);
    Product gold4 = new Product(4,"gold",0);

    Product diamond1 = new Product(3,"diamond",0);
    Product diamond2 = new Product(3,"diamond",0);
    Product diamond3 = new Product(3,"diamond",0);

    List<Object> toModify = new ArrayList<>();
    toModify.add(gold1);
    toModify.add(gold2);
    toModify.add(gold3);
    toModify.add(gold4);
    toModify.add(diamond1);
    toModify.add(diamond2);
    toModify.add(diamond3);


    for(Object obj : toModify)
    {
      factRule.put(obj, new ArrayList<Rule>());
    }

    kieSession.execute(toModify);
    Assert.assertEquals(2,factRule.get(gold2).size());
    Assert.assertEquals(3,ruleCount.size());
  }

  @Test
  public void RulesFiredListenerNoRulesMatch() throws Exception {
    initialize();

    Product gold1 = new Product(1,"gold1",0);
    Product gold2 = new Product(21,"gold1",0);
    Product gold3 = new Product(3,"gold1",0);
    Product gold4 = new Product(4,"gold1",0);

    Product diamond1 = new Product(3,"diamond1",0);
    Product diamond2 = new Product(3,"diamond1",0);
    Product diamond3 = new Product(3,"diamond1",0);

    List<Object> toModify = new ArrayList<>();
    toModify.add(gold1);
    toModify.add(gold2);
    toModify.add(gold3);
    toModify.add(gold4);
    toModify.add(diamond1);
    toModify.add(diamond2);
    toModify.add(diamond3);


    for(Object obj : toModify)
    {
      factRule.put(obj, new ArrayList<Rule>());
    }

    kieSession.execute(toModify);
    Assert.assertEquals(0,factRule.get(gold2).size());
    Assert.assertEquals(0,ruleCount.size());
  }

}
