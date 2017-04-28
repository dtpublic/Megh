package com.datatorrent.drools.rules.DroolListener;

import java.util.List;
import java.util.Map;

import org.kie.api.definition.rule.Rule;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.AgendaGroupPoppedEvent;
import org.kie.api.event.rule.AgendaGroupPushedEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.MatchCancelledEvent;
import org.kie.api.event.rule.MatchCreatedEvent;
import org.kie.api.event.rule.RuleFlowGroupActivatedEvent;
import org.kie.api.event.rule.RuleFlowGroupDeactivatedEvent;

public class RulesFiredListener implements AgendaEventListener{

  Map<Object,List<Rule>> factRule;
  Map<Rule,Integer> ruleCount;

  public RulesFiredListener(Map<Object, List<Rule>> factRule, Map<Rule, Integer> ruleCount)
  {
    this.factRule = factRule;
    this.ruleCount = ruleCount;
  }

  RulesFiredListener()
  {

  }
  public Map<Rule, Integer> getRuleCount()
  {
    return ruleCount;
  }

  public void setRuleCount(Map<Rule, Integer> ruleCount)
  {
    this.ruleCount = ruleCount;
  }

  public Map<Object, List<Rule>> getFactRule()
  {
    return factRule;
  }

  public void setFactRule(Map<Object, List<Rule>> factRule)
  {
    this.factRule = factRule;
  }

  public void matchCreated(MatchCreatedEvent event) {}
  public void matchCancelled(MatchCancelledEvent event) {}
  public void beforeMatchFired(BeforeMatchFiredEvent event)
  {
    Rule matchedRule = event.getMatch().getRule();
    for (Object o : event.getMatch().getObjects()) {
      List<Rule> tempList = factRule.get(o);
      tempList.add(matchedRule);
    }
    Integer toIncrement = ruleCount.get(matchedRule);
    if(toIncrement != null ) {
      toIncrement++;
    } else {
      toIncrement = 1;
    }
    ruleCount.put(matchedRule,toIncrement);

  }
  public void afterMatchFired(AfterMatchFiredEvent event) {

  }

  public void agendaGroupPushed(AgendaGroupPushedEvent event) {}
  public void agendaGroupPopped(AgendaGroupPoppedEvent event) {}
  public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent event) {}
  public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent event) {}
  public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent event) {}
  public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent event) {}
}
