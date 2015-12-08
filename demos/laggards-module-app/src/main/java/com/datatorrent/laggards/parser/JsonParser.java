/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.demos.laggards.parser;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class JsonParser<T> extends BaseOperator
{
  public transient DefaultInputPort<String> in = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      convertToPojo(s);
    }
  };

  /**
   * Converted values are emitted on this port.
   */
  public transient DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  /**
   * Port for emitting error tuples.
   */
  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<String> errPort = new DefaultOutputPort<String>();

  private Class<T> klass;
  private transient ObjectReader reader;

  public void setOutputClass(Class<T> klass)
  {
    this.klass = klass;
  }

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    ObjectMapper mapper = new ObjectMapper();
    reader = mapper.reader(klass);
  }

  protected void convertToPojo(String s)
  {
    try {
      T obj = reader.readValue(s);
      emitTuple(obj);
    } catch (IOException ex) {
      handleError(s);
    }
  }

  private void handleError(String s)
  {
    errPort.emit(s);
  }

  private void emitTuple(T obj)
  {
    out.emit(obj);
  }
}
