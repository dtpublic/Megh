package com.datatorrent.alerts.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.alerts.notification.email.EmailConf;
import com.datatorrent.alerts.notification.email.EmailInfo;
import com.google.common.collect.Lists;

/**
 * make sure the loadConfig() only execute in one thread, and all other
 * operations are read-only to avoid lock/unlock
 */
public abstract class EmailConfigRepo {
  
  public static enum MatchLevel
  {
    MATCH_ALL,
    MATCH_ANY,
    MATCH_NONE;
    
    public static final MatchLevel[] ordedValues = { MATCH_ALL, MATCH_ANY, MATCH_NONE};
  }

  public static class EmailConfigCondition {
    public static EmailConfigCondition DEFAULT = new EmailConfigCondition();

    private String app;
    private Integer level;

    public EmailConfigCondition() {
    }

    public EmailConfigCondition(String app) {
      setApp(app);
    }

    public EmailConfigCondition(int level) {
      this.level = level;
    }

    public EmailConfigCondition(String app, int level) {
      setApp(app);
      this.level = level;
    }

    public void setApp(String app) {
      if (app != null)
        this.app = app.toLowerCase().trim();
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 41 * hash + (app != null ? app.hashCode() : 0);
      hash = 41 * hash + (level != null ? level.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final EmailConfigCondition other = (EmailConfigCondition) obj;
      if (this.app != other.app && (this.app == null || !this.app.equals(other.app))) {
        return false;
      }
      if (this.level != other.level && (this.level == null || !this.level.equals(other.level))) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return String.format("app: %s, level: %d", app, level);
    }
  }

  //public static final String ANY_APP = "";
  // public static final String ANY_TOPIC = "";
  //public static final int ANY_LEVEL = -1;

  protected final Map<EmailConfigCondition, EmailConf> emailConfMap = new HashMap<EmailConfigCondition, EmailConf>();
  protected boolean defaultEmailConfCached = false;
  protected List<EmailConf> defaultEmailConf;

  public abstract void loadConfig();

  public List<EmailConf> getEmailConfig(String appName, int level, MatchLevel matchLevel) {
    if (emailConfMap == null)
      throw new AlertsConfigException("Email Config Map is null, initialize first.");

    switch( matchLevel )
    {
    case MATCH_ALL:
      EmailConf conf = emailConfMap.get(new EmailConfigCondition(appName, level));
      return (conf != null) ? Lists.newArrayList(conf) : null;
    
    case MATCH_ANY:
      EmailConf conf1 = emailConfMap.get(new EmailConfigCondition(appName));
      EmailConf conf2 = emailConfMap.get(new EmailConfigCondition(level));
      return (conf1 != null || conf2 != null) ? Lists.newArrayList(conf1, conf2) : null;

    case MATCH_NONE:
      return getDefaultEmailConfig();
    }
    throw new IllegalArgumentException("Supported match level.");
  }

  public List<EmailConf> getDefaultEmailConfig() {
    if (!defaultEmailConfCached) {
      synchronized (this) {
        if (!defaultEmailConfCached) {
          defaultEmailConf = Lists.newArrayList(emailConfMap.get(EmailConfigCondition.DEFAULT));
          defaultEmailConfCached = true;
        }
      }
    }

    return defaultEmailConf;
  }
  
  /**
   * criteria:
   *   - go to the next level of less matcher only there there don't have any complete email info for this level
   *   - if there are any email info which is complete is one level, will not go to the next level of less match. discard the incompleted match in this level
   *   - the info which fetch from the better matcher should by pass the one from the less matcher.
   *   
   * @param appName
   * @param level
   * @param inputEmailInfo
   * @return
   */
  public List<EmailInfo> fillEmailInfo(String appName, int level, EmailInfo inputEmailInfo)
  {
    if( inputEmailInfo.isComplete() )
      return Lists.newArrayList( inputEmailInfo );
   
    List<EmailInfo> preEmailInfos = Lists.newArrayList(inputEmailInfo);
    for(MatchLevel matchLevel : MatchLevel.ordedValues)
    {
      List<EmailConf> emailConfs = getEmailConfig(appName, level, matchLevel);
      if(emailConfs == null || emailConfs.isEmpty())
        continue;
     
      List<EmailInfo> emailInfos = Lists.newArrayList();
      for(int confIndex=0; confIndex<emailConfs.size(); ++confIndex)
      {
        for(int infoIndex=0; infoIndex<preEmailInfos.size(); ++infoIndex)
        {
          EmailInfo preEmailInfo = ( confIndex+1 == emailConfs.size() ) ? preEmailInfos.get(infoIndex) : preEmailInfos.get(infoIndex).clone();
          emailInfos.add( preEmailInfo.mergeWith(emailConfs.get(confIndex)) );
        }
      }
      
      //check if any complete info
      List<EmailInfo> completeEmailInfos = null;
      for(EmailInfo emailInfo : emailInfos)
      {
        if( emailInfo.isComplete() )
        {
          if( completeEmailInfos == null )
            completeEmailInfos = Lists.newArrayList( emailInfo );
          else
            completeEmailInfos.add(emailInfo);
        }
      }
      if(completeEmailInfos != null)
        return completeEmailInfos;
      
      preEmailInfos = emailInfos;
    }
   
    return null;
  }
}
