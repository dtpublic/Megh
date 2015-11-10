/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedToPojo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class VerifierOperator extends BaseOperator
{

  public final transient DefaultInputPort<Object> valid = new DefaultInputPort<Object>()
  {

    @Override
    public void process(Object tuple)
    {
      Ad ad = (Ad)tuple;
      if (ad.getValid().equals("no")) {
        logger.info("Valid test case fail: {}", ad.toString());
      } else {
        logger.info("Valid test case pass: {}", ad.toString());
      }
    }
  };

  public final transient DefaultInputPort<byte[]> error = new DefaultInputPort<byte[]>()
  {

    @Override
    public void process(byte[] tuple)
    {
      String tupleString = new String(tuple);
      String s[] = tupleString.split(",");
      if (s[s.length - 1].equals("yes")) {
        logger.info("Error test case fail: {}", tupleString);
      } else {
        logger.info("Error test case pass: {}", tupleString);
      }
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(VerifierOperator.class);

}
