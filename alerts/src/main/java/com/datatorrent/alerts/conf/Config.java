package com.datatorrent.alerts.conf;

import java.util.HashMap;
 import java.util.Map;

public class Config {
    private Map<Integer, Integer> waitTime = new HashMap<>() ;
    private final int DefaultTimeForEscalation = 10000 ;
    private int maxLevel = 3;

    public Config() {

        waitTime.put(1, 20000);
        waitTime.put(2, 30000);
        waitTime.put(3, 40000);
    }

    public Integer MaxLevel() {
        return maxLevel;
    }

    public Integer WaitTimeForEscalation(Integer level) {

        if ( waitTime.containsKey(level) ) return waitTime.get(level) ;

        return DefaultTimeForEscalation ;
    }

    //TODO: Set level-timeout mapping
    //TODO: Admin defined configurations
}
