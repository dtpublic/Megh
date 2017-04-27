package com.datatorrent.drools.operator;

import java.io.IOException;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.drools.rules.DroolsRulesReader;
import com.datatorrent.drools.rules.RulesReader;

public class DroolsRuleOperator extends BaseOperator implements ActivationListener<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(DroolsRuleOperator.class);
  private transient KieSession kieSession;
  private String rulesDir;
  private boolean loadSpringSession = false;
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
    if (loadSpringSession) {
      //TODO: support loading session from config where rules are present in classpath
    } else {
      kieSession = initializeKieSessionFromRulesDir();
    }
  }

  private KieSession initializeKieSessionFromRulesDir()
  {
    try {
      RulesReader rulesReader = getRulesReader();
      rulesReader.loadRulesFromDirectory(rulesDir);
    } catch (IOException e) {
      LOG.error("Error loading rules.", e);
      throw new RuntimeException("Error loading rules. ", e);
    }
    KieServices ks = KieServices.Factory.get();
    KieRepository kr = ks.getRepository();
    KieContainer kieContainer = ks.newKieContainer(kr.getDefaultReleaseId());
    //TODO: make kiebase part of state so we don't have to load rules each time we restart operator.
    KieBase kieBase = kieContainer.getKieBase();
    return kieBase.newKieSession();
  }

  protected RulesReader getRulesReader()
  {
    return new DroolsRulesReader();
  }

  @Override
  public void deactivate()
  {
    kieSession.dispose();
    kieSession.destroy();
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
}
