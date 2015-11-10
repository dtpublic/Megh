/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanReader;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.converter.Converter;
import com.datatorrent.contrib.delimited.DelimitedSchema.FIELD_TYPE;
import com.datatorrent.contrib.delimited.DelimitedSchema.Field;
import com.datatorrent.lib.util.ReusableStringReader;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts a delimited tuple to pojo <br>
 * Schema needs to be is specified in a json file as per {@link DelimitedSchema}
 * that contains delimiter and field information<br>
 * Within the schema,date format for date field needs to be provided. Default is
 * dd/MM/yyyy <br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schemaPath</b>:Complete path of schema file in HDFS <br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>input</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>output</b>:POJO<br>
 * <b>error</b>:tuples that could not be converted to pojo
 * 
 * @displayName DelimitedToPojoConverterOperator
 * @category converters
 * @tags delimited converter pojo
 */
public class DelimitedToPojoConverterOperator extends AbstractDelimitedProcessorOperator<byte[], Object, byte[]>
    implements Converter<byte[], Object>
{

  /**
   * Reader to read delimited record
   */
  private transient CsvBeanReader csvBeanReader;
  /**
   * Reader used by csvBeanReader
   */
  private transient ReusableStringReader csvStringReader;
  /**
   * POJO class
   */
  protected Class<?> clazz;

  @AutoMetric
  long inputRecords;
  @AutoMetric
  long convertedRecords;
  @AutoMetric
  long errorRecords;

  @Override
  public void beginWindow(long windowId)
  {
    inputRecords = 0;
    convertedRecords = 0;
    errorRecords = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    csvStringReader = new ReusableStringReader();
    csvBeanReader = new CsvBeanReader(csvStringReader, preference);
  }

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  /**
   * Returns array of cellprocessors, one for each field
   */
  @Override
  protected CellProcessor[] getProcessor(List<Field> fields)
  {
    CellProcessor[] processor = new CellProcessor[fields.size()];
    int fieldCount = 0;
    for (Field field : fields) {
      if (field.getType().equals(FIELD_TYPE.STRING)) {
        processor[fieldCount] = CellProcessorBuilder.getStringCellProcessor();

      } else if (field.getType().equals(FIELD_TYPE.INTEGER)) {
        processor[fieldCount] = CellProcessorBuilder.getIntegerCellProcessor();

      } else if (field.getType().equals(FIELD_TYPE.LONG)) {
        processor[fieldCount] = CellProcessorBuilder.getLongCellProcessor();

      } else if (field.getType().equals(FIELD_TYPE.FLOAT) || field.getType().equals(FIELD_TYPE.DOUBLE)) {
        processor[fieldCount] = CellProcessorBuilder.getDoubleCellProcessor();

      } else if (field.getType().equals(FIELD_TYPE.CHARACTER)) {
        processor[fieldCount] = CellProcessorBuilder.getCharCellProcessor();

      } else if (field.getType().equals(FIELD_TYPE.BOOLEAN)) {
        processor[fieldCount] = CellProcessorBuilder.getBooleanCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.TRUE_VALUE) == null ? null : (String)field.getConstraints().get(
                DelimitedSchema.TRUE_VALUE), field.getConstraints().get(DelimitedSchema.FALSE_VALUE) == null ? null
                : (String)field.getConstraints().get(DelimitedSchema.FALSE_VALUE));

      } else if (field.getType().equals(FIELD_TYPE.DATE)) {
        processor[fieldCount] = CellProcessorBuilder.getDateCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.DATE_FORMAT) == null ? null : (String)field.getConstraints()
                .get(DelimitedSchema.DATE_FORMAT));
      } else {
        processor[fieldCount] = null;
      }
      fieldCount++;
    }
    return processor;
  }

  @Override
  protected void processTuple(byte[] inputTuple)
  {
    Object tuple = convert(inputTuple);
    if (tuple == null) {
      if (error.isConnected()) {
        error.emit(inputTuple);
      }
    } else {
      if (output.isConnected()) {
        output.emit(tuple);
      }
    }

  }

  /**
   * Converts input record to a pojo.
   */
  @Override
  public Object convert(byte[] tuple)
  {
    inputRecords++;
    if (tuple == null) {
      errorRecords++;
      return null;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, header)) {
      errorRecords++;
      return null;
    }
    try {
      csvStringReader.open(incomingString);
      Object obj = csvBeanReader.read(clazz, nameMapping, processors);
      convertedRecords++;
      return obj;
    }catch (SuperCsvException e) {
      logger.error("Error during conversion {} ", e.getMessage());
      errorRecords++;
      return null;
    } catch (IOException e) {
      logger.error("Exception in convert method {}", e.getMessage());
      DTThrowable.rethrow(e);
    }
    return null;
  }

  @Override
  public void teardown()
  {
    try {
      csvBeanReader.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Get the class of pojo
   * 
   * @return Class<?>
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * Set the class of pojo
   * 
   * @param clazz
   */
  @VisibleForTesting
  public void setClazz(Class<?> clazz)
  {
    this.clazz = clazz;
  }

  private static final Logger logger = LoggerFactory.getLogger(DelimitedToPojoConverterOperator.class);

}
