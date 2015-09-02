package com.datatorrent.alerts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Method to return timeout
// TODO: How can the user send the message format ?

public class Message {

    private String appId;
    private String eventId ;
    private Integer currentLevel ;
    private boolean flag ;

    private Map<Integer, Policy> escalationPolicy = new HashMap<>() ;

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

    public Map<Integer, Policy> getEscalationPolicy() {
        return escalationPolicy;
    }

    public void setEscalationPolicy(Map<Integer, Policy> escalationPolicy) {
        this.escalationPolicy = escalationPolicy;
    }

    public Integer timeOutForLevel( Integer level ) {

        if ( escalationPolicy != null ) {

            if ( escalationPolicy.containsKey(level) ) {

                return escalationPolicy.get(level).timeout ;
            }
        }

        return null ;
    }

    public Integer timeOutForCurrLevel( ) {

        return timeOutForLevel(currentLevel) ;
    }

    public List<Action> getCurrentActions() {

        if ( escalationPolicy != null ) {

            if ( escalationPolicy.containsKey(currentLevel) )
                return escalationPolicy.get(currentLevel).actions ;
        }

        return null ;
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
