/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedparser;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource("test-properties.xml");
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(5000); // runs for 5 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
