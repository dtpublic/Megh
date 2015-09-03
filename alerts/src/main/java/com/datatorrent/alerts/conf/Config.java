package com.datatorrent.alerts.conf;

import com.datatorrent.alerts.Policy;

import java.util.HashMap;
 import java.util.Map;

// TODO :Application specific config file or Site specific
public class Config {

    public Map<Integer, Policy> getEscalationPolicy(){

        Map<Integer, Policy> fromXml = new HashMap<>() ;


        return fromXml ;
    }

}
