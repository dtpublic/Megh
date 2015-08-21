package com.datatorrent.alerts;

import org.junit.Test;

import com.datatorrent.alerts.conf.DefaultEmailConfigRepo;

public class EmailConfigureTester {

  @Test
  public void testLoadConfigure()
  {
    DefaultEmailConfigRepo repo = DefaultEmailConfigRepo.instance();
  }
}
