package com.datatorrent.alerts.conf;

public interface Config {
    Integer MaxLevel() ;
    Integer WaitTimeForEscalation(Integer Level) ;
}
