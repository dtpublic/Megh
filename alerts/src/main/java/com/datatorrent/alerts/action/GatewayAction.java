package com.datatorrent.alerts.action;

import com.datatorrent.certification.DTGatewayProxy;

public class GatewayAction {

  private DTGatewayProxy proxy;
  
  public GatewayAction(String gatewayAddress)
  {
    setGatewayAddress(gatewayAddress);
  }
  
  public void killApp(String appId)
  {
    
  }
  

  public void setGatewayAddress(String gatewayAddress) {
    proxy = new DTGatewayProxy(gatewayAddress);
  }
  
  
}
