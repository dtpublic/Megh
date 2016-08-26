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

  public final transient DefaultInputPort<KeyValPair<String, String>> error = new DefaultInputPort<KeyValPair<String, String>>()
  {

    @Override
    public void process(KeyValPair<String, String> tuple)
    {
      String tupleString = new String(tuple.getKey());
      String[] s = tupleString.split(",");
      if (s[s.length - 1].equals("yes")) {
        logger.info("Error Case Test fail: {}", tuple);
      } else {
        logger.info("Error Case Test pass:{}", tuple);
      }
    }
  };

  public final transient DefaultInputPort<Map<String, Object>> parsedObject = new DefaultInputPort<Map<String, Object>>()
  {

    @Override
    public void process(Map<String, Object> tuple)
    {
      String tupleString = (String)tuple.get("valid");
      if (tupleString.equals("no")) {
        logger.info("Valid case Object Test fail: {}", tuple);
      } else {
        logger.info("Valid case Object Test pass: {}", tuple);
      }
    }
  };
  
  public final transient DefaultInputPort<Object> pojo = new DefaultInputPort<Object>()
      {

        @Override
        public void process(Object tuple)
        {
          Ad ad = (Ad)tuple;
          if (ad.getValid().equals("no")) {
            logger.info("Valid case pojo Test fail: {}", ad);
          } else {
            logger.info("Valid case pojo Test pass: {}", ad);
          }
        }
      };

  private static final Logger logger = LoggerFactory.getLogger(VerifierOperator.class);
}
