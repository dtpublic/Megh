package com.datatorrent.drools.operator;

import java.io.IOException;

import org.kie.api.KieServices;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.drools.rules.DroolsRulesReader;
import com.datatorrent.drools.rules.RulesReader;

public class StatelessDroolsOperator extends BaseOperator implements ActivationListener<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(StatelessDroolsOperator.class);
  private transient StatelessKieSession kieSession;
  private String rulesDir;
  private boolean loadSpringSession = false;
  private String sessionName;
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
}
