/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvMapReader;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.delimited.DelimitedSchema.FIELD_TYPE;
import com.datatorrent.contrib.delimited.DelimitedSchema.Field;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.ReusableStringReader;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that parses a delimited tuple against a specified schema <br>
 * Schema is specified in a json file as per {@link DelimitedSchema} that
 * contains field information and constraints for each field <br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schemaPath</b>:Complete path of schema file in HDFS <br>
 * <b>Ports</b> <br>
 * <b>input</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>validatedData</b>:tuples that are validated against the schema are emitted
 * as byte[] on this port<br>
 * <b>output</b>:tuples that are validated against the schema are emitted as
 * Map<String,Object> on this port<br>
 * <b>error</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 * 
 * @displayName DelimitedParserOperator
 * @category Parsers
 * @tags delimited parser
 */
public class DelimitedParserOperator extends
    AbstractDelimitedProcessorOperator<byte[], Map<String, Object>, KeyValPair<String, String>>
{

  /**
   * Reader to read delimited records
   */
  private transient CsvMapReader csvMapReader;
  /**
   * Reader used by csvMapReader
   */
  private transient ReusableStringReader csvStringReader;

  @AutoMetric
  long inputRecords;
  @AutoMetric
  long validRecords;
  @AutoMetric
  long parsedRecords;
  @AutoMetric
  long errorRecords;

  @Override
  public void beginWindow(long windowId)
  {
    inputRecords = 0;
    validRecords = 0;
    parsedRecords = 0;
    errorRecords = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    csvStringReader = new ReusableStringReader();
    csvMapReader = new CsvMapReader(csvStringReader, preference);
  }

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
        processor[fieldCount] = CellProcessorBuilder.getStringCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.LENGTH) == null ? null : Integer.parseInt((String)field
                .getConstraints().get(DelimitedSchema.LENGTH)),
            field.getConstraints().get(DelimitedSchema.MIN_LENGTH) == null ? null : Integer.parseInt((String)field
                .getConstraints().get(DelimitedSchema.MIN_LENGTH)),
            field.getConstraints().get(DelimitedSchema.MAX_LENGTH) == null ? null : Integer.parseInt((String)field
                .getConstraints().get(DelimitedSchema.MAX_LENGTH)),
            field.getConstraints().get(DelimitedSchema.EQUALS) == null ? null : (String)field.getConstraints().get(
                DelimitedSchema.EQUALS), field.getConstraints().get(DelimitedSchema.REGEX_PATTERN) == null ? null
                : (String)field.getConstraints().get(DelimitedSchema.REGEX_PATTERN));
      } else if (field.getType().equals(FIELD_TYPE.INTEGER)) {
        processor[fieldCount] = CellProcessorBuilder.getIntegerCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.EQUALS) == null ? null : Integer.parseInt((String)field
                .getConstraints().get(DelimitedSchema.EQUALS)));
      } else if (field.getType().equals(FIELD_TYPE.LONG)) {
        processor[fieldCount] = CellProcessorBuilder.getLongCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.EQUALS) == null ? null : Long.parseLong((String)field
                .getConstraints().get(DelimitedSchema.EQUALS)),
            field.getConstraints().get(DelimitedSchema.MIN_VALUE) == null ? null : Long.parseLong((String)field
                .getConstraints().get(DelimitedSchema.MIN_VALUE)),
            field.getConstraints().get(DelimitedSchema.MAX_VALUE) == null ? null : Long.parseLong((String)field
                .getConstraints().get(DelimitedSchema.MAX_VALUE)));
      } else if (field.getType().equals(FIELD_TYPE.FLOAT) || field.getType().equals(FIELD_TYPE.DOUBLE)) {
        processor[fieldCount] = CellProcessorBuilder.getDoubleCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.EQUALS) == null ? null : Double.parseDouble((String)field
                .getConstraints().get(DelimitedSchema.EQUALS)),
            field.getConstraints().get(DelimitedSchema.MIN_VALUE) == null ? null : Double.parseDouble((String)field
                .getConstraints().get(DelimitedSchema.MIN_VALUE)),
            field.getConstraints().get(DelimitedSchema.MAX_VALUE) == null ? null : Double.parseDouble((String)field
                .getConstraints().get(DelimitedSchema.MAX_VALUE)));
      } else if (field.getType().equals(FIELD_TYPE.CHARACTER)) {
        processor[fieldCount] = CellProcessorBuilder.getCharCellProcessor(
            field.getConstraints().get(DelimitedSchema.REQUIRED) == null ? null : Boolean.parseBoolean((String)field
                .getConstraints().get(DelimitedSchema.REQUIRED)),
            field.getConstraints().get(DelimitedSchema.EQUALS) == null ? null : ((String)field.getConstraints().get(
                DelimitedSchema.EQUALS)).charAt(0));
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

  /**
   * parses incoming delimited record.
   * Blank and null records are treated as error and emitted 
   * on error port with an error message
   * Header is also treated as error and emitted on error port
   * with error message
   */
  @Override
  protected void processTuple(byte[] tuple)
  {
    inputRecords++;
    if (tuple == null) {
      if (error.isConnected()) {
        error.emit(new KeyValPair<String, String>(null, "Blank/null tuple"));
      }
      errorRecords++;
      return;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, header)) {
      if (error.isConnected()) {
        error.emit(new KeyValPair<String, String>(incomingString, "Blank/header tuple"));
      }
      errorRecords++;
      return;
    }
    try {
      csvStringReader.open(incomingString);
      Map<String, Object> map = csvMapReader.read(nameMapping, processors);
      if (output.isConnected()) {
        output.emit(map);
      }
      parsedRecords++;
      if (validatedData.isConnected()) {
        validatedData.emit(tuple);
      }
      validRecords++;
    } catch (SuperCsvException e) {
      if (error.isConnected()) {
        error.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
      }
      errorRecords++;
      logger.error("Tuple could not be parsed. Reason {}", e.getMessage());
    } catch (IOException e) {
      logger.error("Exception in process method {}", e.getMessage());
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      csvMapReader.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  public final transient DefaultOutputPort<byte[]> validatedData = new DefaultOutputPort<byte[]>();
  private static final Logger logger = LoggerFactory.getLogger(DelimitedParserOperator.class);

}
