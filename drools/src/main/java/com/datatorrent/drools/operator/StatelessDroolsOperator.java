package com.datatorrent.drools.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.drools.rules.DroolsRulesReader;
import com.datatorrent.drools.rules.RulesReader;

@InterfaceStability.Evolving
public class StatelessDroolsOperator extends BaseOperator implements ActivationListener<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(StatelessDroolsOperator.class);
  private transient StatelessKieSession kieSession;
  private String rulesDir;
  private boolean loadSpringSession = false;
  private String sessionName;
  private Map<Object, List<Rule>> factsAndFiredRules;
  private Map<Rule, Integer> ruleCount;
  public final transient DefaultInputPort<Object> factsInput = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {

    }
  };

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
      KieContainer kieContainer = KieServices.Factory.get().newKieClasspathContainer();
      kieSession = kieContainer.newStatelessKieSession(sessionName);
    } else {
      KieContainer kieContainer = initializeKieContainerFromRulesDir();
      kieSession = kieContainer.newStatelessKieSession();
    }
    factsAndFiredRules = new HashMap<>();
    ruleCount = new HashMap<>();
    kieSession.addEventListener(new RulesFiredListener());
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
   * @return rulesDir
   */
  public String getRulesDir()
  {
    return rulesDir;
  }

  /**
   * Sets rules directory containing rules files e.g. .drl, .xls files
   * @param rulesDir
   */
  public void setRulesDir(String rulesDir)
  {
    this.rulesDir = rulesDir;
  }

  /**
   * If load kieSession from spring configuration, this reads rules from
   * classpath
   * @return loadSpringSession
   */
  public boolean isLoadSpringSession()
  {
    return loadSpringSession;
  }

  /**
   * If load kieSession from spring configuration, this reads rules from
   * classpath
   * @param loadSpringSession
   */
  public void setLoadSpringSession(boolean loadSpringSession)
  {
    this.loadSpringSession = loadSpringSession;
  }

  /**
   * Get session name to be loaded from spring configuration file
   * @return sessionName
   */
  public String getSessionName()
  {
    return sessionName;
  }

  /**
   * Set session name to be loaded from spring configuration file
   * @param sessionName
   */
  public void setSessionName(String sessionName)
  {
    this.sessionName = sessionName;
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
     * @param afterMatchFiredEvent the event which triggered this call
     */
    @Override
    public void afterMatchFired(AfterMatchFiredEvent afterMatchFiredEvent)
    {
      Rule matchedRule = afterMatchFiredEvent.getMatch().getRule();
      for (Object matchedObject : afterMatchFiredEvent.getMatch().getObjects()) {
        List<Rule> currentRulesList = factsAndFiredRules.get(matchedObject);
        currentRulesList.add(matchedRule);
      }
      Integer matchedRuleCount = ruleCount.get(matchedRule);
      if (matchedRuleCount != null) {
        matchedRuleCount++;
      } else {
        matchedRuleCount = 1;
      }
      ruleCount.put(matchedRule, matchedRuleCount);
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
     *
     * @param objectInsertedEvent fact inserted in the session
     */
    @Override
    public void objectInserted(ObjectInsertedEvent objectInsertedEvent)
    {
      factsAndFiredRules.put(objectInsertedEvent.getObject(), new ArrayList<Rule>());
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
