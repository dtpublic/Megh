package com.datatorrent.alerts;

public interface Config {
    Integer MaxLevel() ;
    Integer WaitTimeForEscalation(Integer Level) ;
}