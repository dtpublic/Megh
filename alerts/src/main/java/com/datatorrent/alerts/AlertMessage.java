package com.datatorrent.alerts;

import java.awt.image.BufferedImage;
import java.util.Comparator;
import java.util.List;

public class AlertMessage implements AlertAction
{
  private String message;

  public String getAppId()
  {
    return appId;
  }

  public void setAppId(String appId)
  {
    this.appId = appId;
  }

  private String appId;

  private Integer eventId;
  private Integer level;
  private List<String> messageReceivers;
  private boolean flag;

  public void setEventId(Integer eventId)
  {
    this.eventId = eventId;
  }

  @Override
  public Integer getEventID()
  {
    return 0;
  }

  public Integer getLevel()
  {
    return level;
  }

  public void setLevel(Integer level)
  {
    this.level = level;
  }

  public boolean isFlag()
  {
    return flag;
  }

  public void setFlag(boolean flag)
  {
    this.flag = flag;
  }

  @Override
  public String getApplicationId()
  {
    return null;
  }

  @Override
  public String getAlertTitle()
  {
    return null;
  }

  @Override
  public String getAlertContent()
  {
    return null;
  }

  @Override
  public BufferedImage getScreenShot()
  {
    return null;
  }

  @Override
  public String getAlertType()
  {
    return null;
  }

  @Override
  public List<EscalationLevel> getEscalationPolicy()
  {
    return null;
  }

  @Override
  public int getInitialAlertLevel()
  {
    return 0;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    AlertMessage that = (AlertMessage) o;

    if (!appId.equals(that.appId))
      return false;
    return eventId.equals(that.eventId);
  }

  @Override
  public int hashCode()
  {
    int result = appId.hashCode();
    result = 31 * result + eventId.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "{" + "appId:" + appId + ", message:" + message + ", eventId: " + eventId + "}";

  }
}
