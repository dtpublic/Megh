/**
 * This class defines the tuple which will send from the alert storage module to the alert action module
 */
package com.datatorrent.alerts;

public class ActionTuple {
  public static enum Action
  {
    NOTIFY_EMAIL,
    RESTART_ME
  }
  
  private Action action;
  private int level;
  private String appName;
  
  
  public Action getAction() {
    return action;
  }
  public void setAction(Action action) {
    this.action = action;
  }
  public int getLevel() {
    return level;
  }
  public void setLevel(int level) {
    this.level = level;
  }
  public String getAppName() {
    return appName;
  }
  public void setAppName(String appName) {
    this.appName = appName;
  }
  
  
}
