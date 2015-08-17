package com.datatorrent.alerts;

import java.util.HashMap;
 import java.util.Map;

public class ConfigImpl implements Config {
    private Map<Integer, Integer> waitTime = new HashMap<>() ;
    private final int DefaultTimeForEscalation = 10000 ;
    private int maxLevel = 3;

    @Override
    public Integer MaxLevel() {
        return maxLevel;
    }

    @Override
    public Integer WaitTimeForEscalation(Integer level) {

        if ( waitTime.containsKey(level) ) return waitTime.get(level) ;

        return DefaultTimeForEscalation ;
    }

    //TODO: Set level-timeout mapping
    //TODO: Admin defined configurations
}