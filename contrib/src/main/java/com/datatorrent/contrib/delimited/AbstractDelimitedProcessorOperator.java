/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.delimited.DelimitedSchema.Field;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Base implementation of DelimitedProcessorOperator.Operators that need to
 * process delimited records can extend this class. This operator reads json
 * schema as specified in {@link DelimitedSchema} Concrete implementations need
 * to provide readers/writers and override {{@link #processTuple} method and {
 * {@link #getProcessor(List)} method.
 * 
 * @param <INPUT>
 *          input type of tuple
 * @param <OUTPUT>
 *          output type of tuple
 * @param <ERROR>
 *          error type of tuple
 */
public abstract class AbstractDelimitedProcessorOperator<INPUT, OUTPUT, ERROR> extends BaseOperator
{
  /**
   * Contents of the schema
   */
  private String schema;
  /**
   * Complete path where schema resides.
   */
  @NotNull
  private transient String schemaPath;
  /**
   * Schema is read into this object to access fields
   */
  protected transient DelimitedSchema delimitedParserSchema;
  /**
   * Cell processors are an integral part of reading and writing with Super CSV
   * they automate the data type conversions, and enforce constraints.
   */
  protected transient CellProcessor[] processors;
  /**
   * Names of all the fields in the same order of incoming records
   */
  protected transient String[] nameMapping;
  /**
   * header-this will be delimiter separated string of field names
   */
  protected transient String header;
  /**
   * Reading/Writing preferences that are passed through schema
   */
  protected transient CsvPreference preference;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    delimitedParserSchema = new DelimitedSchema(schema);
    preference = new CsvPreference.Builder(delimitedParserSchema.getQuoteChar(),
        delimitedParserSchema.getDelimiterChar(), delimitedParserSchema.getLineDelimiter()).build();
    nameMapping = delimitedParserSchema.getFieldNames().toArray(
        new String[delimitedParserSchema.getFieldNames().size()]);
    header = StringUtils.join(nameMapping, (char)delimitedParserSchema.getDelimiterChar() + "");
    processors = getProcessor(delimitedParserSchema.getFields());
  }

  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    public void process(INPUT tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * Concrete implementations need to define CellProcessor each
   * field.CellProcessor take care of validating the constraints for each field
   * during processing.
   * 
   * @param fields
   *          fields that are defined in the schema
   * @return
   */
  abstract protected CellProcessor[] getProcessor(List<Field> fields);

  /**
   * method to process incoming tuples. Concrete classes need to implement this.
   * This method is called by the process method of input port
   * 
   * @param tuple
   */
  abstract protected void processTuple(INPUT tuple);

  /**
   * 
   * @return complete path of schema
   */
  public String getSchemaPath()
  {
    return schemaPath;
  }

  /**
   * Set the schemaPath. Schema is a json file residing in HDFS. Complete path
   * of the schema needs to be specified
   * 
   * @param schemaPath
   *          HDFS location of the file that contains schema
   */
  public void setSchemaPath(String schemaPath)
  {
    this.schemaPath = schemaPath;
    try {
      this.schema = readJsonSchema(schemaPath);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * 
   * @param path
   *          HDFS path to the json schema
   * @return json schema contents
   * @throws IOException
   */
  private static String readJsonSchema(String path) throws IOException
  {
    Path inputPath = new Path(path);
    StringWriter stringWriter = new StringWriter();
    try (FileSystem fs = FileSystem.get(new Configuration()); InputStream is = fs.open(inputPath)) {
      IOUtils.copy(is, stringWriter);
    }
    return stringWriter.toString();
  }

  /**
   * error port to emit records that could not be processed
   */
  public final transient DefaultOutputPort<ERROR> error = new DefaultOutputPort<ERROR>();
  /**
   * output port to emit processed records
   */
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
}
