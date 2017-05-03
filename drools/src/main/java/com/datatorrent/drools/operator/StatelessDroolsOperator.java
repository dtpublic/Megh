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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.kie.api.KieServices;
import org.kie.api.builder.KieRepository;
import org.kie.api.definition.rule.Rule;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.AgendaGroupPoppedEvent;
import org.kie.api.event.rule.AgendaGroupPushedEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.MatchCancelledEvent;
import org.kie.api.event.rule.MatchCreatedEvent;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.event.rule.RuleFlowGroupActivatedEvent;
import org.kie.api.event.rule.RuleFlowGroupDeactivatedEvent;
import org.kie.api.event.rule.RuleRuntimeEventListener;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.drools.rules.DroolsRulesReader;
import com.datatorrent.drools.rules.RulesReader;
import com.datatorrent.drools.utils.DroolUtils;

@InterfaceStability.Evolving
public class StatelessDroolsOperator extends BaseOperator implements ActivationListener<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(StatelessDroolsOperator.class);
  private static final int DEFAULT_BATCH_SIZE = 1000;
  public final transient DefaultOutputPort<Object> factsOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Map<Rule, Integer>> ruleCountOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Map<Object, List<Rule>>> factAndFiredRulesOutput = new DefaultOutputPort<>();
  private transient StatelessKieSession kieSession;
  @NotNull
  private String rulesDir;
  private boolean loadSpringSession = false;
  private String sessionName;
  private transient Map<Object, List<Rule>> factsAndFiredRules;
  private transient Map<Rule, Integer> ruleCount;
  private transient List<Object> facts;
  private transient List<Object> factsFromRules;
  private int batchSize = DEFAULT_BATCH_SIZE;
  public final transient DefaultInputPort<Object> factsInput = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      facts.add(tuple);
      if (facts.size() >= batchSize) {
        executeRules();
      }
    }
  };

  @Override
  public void endWindow()
  {
    if (facts.size() > 0) {
      executeRules();
    }
    if (factAndFiredRulesOutput.isConnected()) {
      factAndFiredRulesOutput.emit(factsAndFiredRules);
    }
    if (ruleCountOutput.isConnected()) {
      ruleCountOutput.emit(ruleCount);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (factAndFiredRulesOutput.isConnected()) {
      factsAndFiredRules.clear();
    }
    if (ruleCountOutput.isConnected()) {
      ruleCount.clear();
    }
  }

  private void executeRules()
  {
    kieSession.execute(facts);
    for (Object fact : facts) {
      factsOutput.emit(fact);
    }
    for (Object fact : factsFromRules) {
      factsOutput.emit(fact);
    }
    facts.clear();
    factsFromRules.clear();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void activate(OperatorContext context)
  {
    //TODO: save kieBase so that we don't we have saved rules state
    if (loadSpringSession) {
      try {
        DroolUtils.addKjarToClasspath(rulesDir);
        KieContainer kieContainer = KieServices.Factory.get().newKieClasspathContainer();
        kieSession = kieContainer.newStatelessKieSession(sessionName);
      } catch (IOException e) {
        throw new RuntimeException("Error loading rules from classpath.", e);
      }
    } else {
      KieContainer kieContainer = initializeKieContainerFromRulesDir();
      kieSession = kieContainer.newStatelessKieSession();
    }
    facts = new ArrayList<>();
    factsFromRules = new ArrayList<>();

    if (factAndFiredRulesOutput.isConnected() || ruleCountOutput.isConnected()) {
      if (factAndFiredRulesOutput.isConnected()) {
        factsAndFiredRules = new HashMap<>();
      }
      if (ruleCountOutput.isConnected()) {
        ruleCount = new HashMap<>();
      }
      kieSession.addEventListener(new RulesFiredListener());
    }
    kieSession.addEventListener(new FactsListener());
  }

  private KieContainer initializeKieContainerFromRulesDir()
  {
    try {
      RulesReader rulesReader = getRulesReader();
      rulesReader.loadRulesFromDirectory(rulesDir);
    } catch (IOException e) {
      LOG.error("Error loading rules.", e);
      throw new RuntimeException("Error loading rules. ", e);
    }
    KieServices kieServices = KieServices.Factory.get();
    KieRepository kieRepository = kieServices.getRepository();
    KieContainer kieContainer = kieServices.newKieContainer(kieRepository.getDefaultReleaseId());
    return kieContainer;
  }

  protected RulesReader getRulesReader()
  {
    return new DroolsRulesReader();
  }

  @Override
  public void deactivate()
  {
  }

  /**
   * Get rules directory containing rules files e.g. .drl, .xls files
   *
   * @return rulesDir
   */
  public String getRulesDir()
  {
    return rulesDir;
  }

  /**
   * Sets rules directory containing rules files e.g. .drl, .xls files
   *
   * @param rulesDir
   */
  public void setRulesDir(String rulesDir)
  {
    this.rulesDir = rulesDir;
  }

  /**
   * If load kieSession from spring configuration, this reads rules from
   * classpath
   *
   * @return loadSpringSession
   */
  public boolean isLoadSpringSession()
  {
    return loadSpringSession;
  }

  /**
   * If load kieSession from spring configuration, this reads rules from
   * classpath
   *
   * @param loadSpringSession
   */
  public void setLoadSpringSession(boolean loadSpringSession)
  {
    this.loadSpringSession = loadSpringSession;
  }

  /**
   * Get session name to be loaded from spring configuration file
   *
   * @return sessionName
   */
  public String getSessionName()
  {
    return sessionName;
  }

  /**
   * Set session name to be loaded from spring configuration file
   *
   * @param sessionName
   */
  public void setSessionName(String sessionName)
  {
    this.sessionName = sessionName;
  }

  /**
   * Getter function to get the batch size
   * @return batchSize
   */
  public int getBatchSize()
{
  return batchSize;
}

  /**
   * Setter function to set the batch size for firing the rules
   * @param batchSize size of the batch
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }


  @VisibleForTesting
  public StatelessKieSession getKieSession()
  {
    return kieSession;
  }

  private class RulesFiredListener implements AgendaEventListener
  {
    @Override
    public void matchCreated(MatchCreatedEvent matchCreatedEvent)
    {

    }

    @Override
    public void matchCancelled(MatchCancelledEvent matchCancelledEvent)
    {

    }

    @Override
    public void beforeMatchFired(BeforeMatchFiredEvent event)
    {

    }

    /**
     * This function will be called automatically after firing a Rule.
     * Modifies the map factsAndFiredRules and ruleCount.
     *
     * @param afterMatchFiredEvent the event which triggered this call
     */
    @Override
    public void afterMatchFired(AfterMatchFiredEvent afterMatchFiredEvent)
    {
      Rule matchedRule = afterMatchFiredEvent.getMatch().getRule();
      if(factAndFiredRulesOutput.isConnected()) {
        for (Object matchedObject : afterMatchFiredEvent.getMatch().getObjects()) {
          List<Rule> currentRulesList = factsAndFiredRules.get(matchedObject);
          currentRulesList.add(matchedRule);
        }
      }
      if(ruleCountOutput.isConnected()) {
        Integer matchedRuleCount = ruleCount.get(matchedRule);
        if (matchedRuleCount != null) {
          matchedRuleCount++;
        } else {
          matchedRuleCount = 1;
        }
        ruleCount.put(matchedRule, matchedRuleCount);
      }
    }

    @Override
    public void agendaGroupPopped(AgendaGroupPoppedEvent agendaGroupPoppedEvent)
    {

    }

    @Override
    public void agendaGroupPushed(AgendaGroupPushedEvent agendaGroupPushedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }
  }

  /**
   * Listener to track when new facts are added in the Session.
   * Required because even rules can add new facts to the session.
   */
  private class FactsListener implements RuleRuntimeEventListener
  {
    /**
     * This method will be called when a fact is inserted in the session.
     * This method will add the inserted fact in factsAndFiredRules Map.
     * When the getRule() will not be null in that case the fact is added via a rule.
     *
     * @param objectInsertedEvent fact inserted in the session
     */
    @Override
    public void objectInserted(ObjectInsertedEvent objectInsertedEvent)
    {
      Object newFact = objectInsertedEvent.getObject();
      if (factAndFiredRulesOutput.isConnected() && !factsAndFiredRules.containsKey(newFact)) {
        factsAndFiredRules.put(newFact, new ArrayList<Rule>());
      }
      if (objectInsertedEvent.getRule() != null) {
        factsFromRules.add(newFact);
      }
    }

    @Override
    public void objectUpdated(ObjectUpdatedEvent objectUpdatedEvent)
    {

    }

    @Override
    public void objectDeleted(ObjectDeletedEvent objectDeletedEvent)
    {

    }
  }
}
