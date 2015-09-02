package com.datatorrent.alerts;

import java.util.HashMap;
import java.util.Map;

public class Message {

    private String appId;
    private String eventId ;
    private Integer currentLevel ;
    private boolean flag ;

    private Map<Integer, Action> EscalationPolicy = new HashMap<>() ;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Integer getCurrentLevel() {
        return currentLevel;
    }

    public void setCurrentLevel(Integer currentLevel) {
        this.currentLevel = currentLevel;
    }

    public Map<Integer, Action> getEscalationPolicy() {
        return EscalationPolicy;
    }

    public void setEscalationPolicy(Map<Integer, Action> escalationPolicy) {
        EscalationPolicy = escalationPolicy;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Message that = (Message) o;

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
        return "{" + "appId:" + appId +  ", eventId: " + eventId + "}";

    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
