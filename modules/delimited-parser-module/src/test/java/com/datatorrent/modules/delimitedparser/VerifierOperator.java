/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedparser;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class VerifierOperator extends BaseOperator
{

  public final transient DefaultInputPort<byte[]> valid = new DefaultInputPort<byte[]>()
  {

    @Override
    public void process(byte[] tuple)
    {
      String tupleString = new String(tuple);
      String[] s = tupleString.split(",");
      if (s[s.length - 1].equals("no")) {
        logger.info("Valid case AS-IS Test fail: {}", tupleString);
      } else {
        logger.info("Valid case AS-IS Test pass: {}", tupleString);
      }
    }
  };

  public final transient DefaultInputPort<KeyValPair<String, String>> error = new DefaultInputPort<KeyValPair<String, String>>()
  {

    @Override
    public void process(KeyValPair<String, String> tuple)
    {
      String tupleString = new String(tuple.getKey());
      String[] s = tupleString.split(",");
      if (s[s.length - 1].equals("yes")) {
        logger.info("Error Case Test fail: {}", tupleString);
      } else {
        logger.info("Error Case Test pass:{}", tupleString);
      }
    }
  };

  public final transient DefaultInputPort<Map<String, Object>> parsedObject = new DefaultInputPort<Map<String, Object>>()
  {

    @Override
    public void process(Map<String, Object> tuple)
    {
      String tupleString = (String)tuple.get("valid");
      String[] s = tupleString.split(",");
      if (s[s.length - 1].equals("no")) {
        logger.info("Valid case Object Test fail: {}", tupleString);
      } else {
        logger.info("Valid case Object Test pass: {}", tupleString);
      }
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(VerifierOperator.class);
}
