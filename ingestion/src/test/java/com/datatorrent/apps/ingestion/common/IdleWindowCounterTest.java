/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class IdleWindowCounterTest
{
  public static class ConcreteIdleWindowCounter extends IdleWindowCounter{
    
    boolean triggerReceived = false;

    @Override
    protected boolean hasMoreWork()
    {
      return false;
    }

    @Override
    protected void idleWindowThresholdReached()
    {
      triggerReceived = true;
    }
    
    @Override
    protected int getIdleWindowThresholdDefault()
    {
      return 50;
    }
  }
  
  @Test
  public void testIdleTimeCounterForActivity(){
    ConcreteIdleWindowCounter idleWindowCounter = new ConcreteIdleWindowCounter();
    
    //Test 1: Some activity done at Window 25.
    //So trigger wont be set till window 25+50
    for(int i=0; i< 70; i++){
      idleWindowCounter.beginWindow(i);
      if(i==25){
        idleWindowCounter.markActivity();
      }
      idleWindowCounter.endWindow();
    }
    Assert.assertFalse(idleWindowCounter.triggerReceived);
    
  }
  
  @Test
  public void testIdleTimeCounter(){
    ConcreteIdleWindowCounter idleWindowCounter = new ConcreteIdleWindowCounter();
    //Idle for 50 winodows. So trigger should be set
    for(int i=0; i<= 50; i++){
      idleWindowCounter.beginWindow(i);
      idleWindowCounter.endWindow();
      if(i < 50){
        Assert.assertFalse(idleWindowCounter.triggerReceived);
      }
      else{
        Assert.assertTrue(idleWindowCounter.triggerReceived);
      }
    }
    
  }
}
