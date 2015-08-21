package com.datatorrent.alerts.conf;

import java.util.List;
import java.util.Map;

import com.datatorrent.alerts.notification.email.EmailConf;
import com.google.common.collect.Lists;

/**
 * make sure the loadConfig() only execute in one thread, and all other
 * operations are read-only to avoid lock/unlock
 */
public abstract class EmailConfigRepo {

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

  }

  public static final String ANY_APP = "";
  // public static final String ANY_TOPIC = "";
  public static final int ANY_LEVEL = -1;

  protected Map<EmailConfigCondition, EmailConf> emailConfMap;
  protected boolean defaultEmailConfCached = false;
  protected List<EmailConf> defaultEmailConf;

  public abstract void loadConfig();

  public List<EmailConf> getEmailConfig(String appName, int level) {
    if (emailConfMap == null)
      throw new AlertsConfigException("Email Config Map is null, initialize first.");

    // search the most match
    EmailConf conf = emailConfMap.get(new EmailConfigCondition(appName, level));
    if (conf != null)
      return Lists.newArrayList(conf);

    // search the only one match
    EmailConf conf1 = emailConfMap.get(new EmailConfigCondition(appName));
    EmailConf conf2 = emailConfMap.get(new EmailConfigCondition(level));
    if (conf1 != null || conf2 != null)
      return Lists.newArrayList(conf1, conf2);

    // search default
    return getDefaultEmailConfig();

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
}
