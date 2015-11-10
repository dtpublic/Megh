/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.parser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.parser.DelimitedSchema.Field;
import com.datatorrent.contrib.utils.FileUtils;
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
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>input</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>output</b>:tuples that are validated against the schema are emitted as
 * Map<String,Object> on this port<br>
 * <b>pojo</b>:tuples that are validated against the schema are emitted as pojo
 * on this port<br>
 * <b>error</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 * 
 * @displayName DelimitedParserOperator
 * @category Parsers
 * @tags delimited parser
 */
public class DelimitedParserOperator extends BaseOperator
{

  /**
   * Map Reader to read delimited records
   */
  private transient CsvMapReader csvMapReader;
  /**
   * Bean Reader to read delimited records
   */
  private transient CsvBeanReader csvBeanReader;
  /**
   * Reader used by csvMapReader and csvBeanReader
   */
  private transient ReusableStringReader csvStringReader;
  /**
   * POJO class
   */
  private Class<?> clazz;
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
  private transient DelimitedSchema delimitedParserSchema;
  /**
   * Cell processors are an integral part of reading and writing with Super CSV
   * they automate the data type conversions, and enforce constraints.
   */
  private transient CellProcessor[] processors;
  /**
   * Names of all the fields in the same order of incoming records
   */
  private transient String[] nameMapping;
  /**
   * header-this will be delimiter separated string of field names
   */
  private transient String header;
  /**
   * Reading preferences that are passed through schema
   */
  private transient CsvPreference preference;

  @AutoMetric
  long inputRecords;
  @AutoMetric
  long parsedRecords;
  @AutoMetric
  long pojoRecords;
  @AutoMetric
  long errorRecords;

  @Override
  public void beginWindow(long windowId)
  {
    inputRecords = 0;
    parsedRecords = 0;
    errorRecords = 0;
    pojoRecords = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    delimitedParserSchema = new DelimitedSchema(schema);
    preference = new CsvPreference.Builder(delimitedParserSchema.getQuoteChar(),
        delimitedParserSchema.getDelimiterChar(), delimitedParserSchema.getLineDelimiter()).build();
    nameMapping = delimitedParserSchema.getFieldNames().toArray(
        new String[delimitedParserSchema.getFieldNames().size()]);
    header = StringUtils.join(nameMapping, (char)delimitedParserSchema.getDelimiterChar() + "");
    processors = getProcessor(delimitedParserSchema.getFields());
    csvStringReader = new ReusableStringReader();
    csvMapReader = new CsvMapReader(csvStringReader, preference);
    csvBeanReader = new CsvBeanReader(csvStringReader, preference);
  }

  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    /**
     * parses incoming delimited record and emits map or pojo.Error records are
     * emitted on error port with reason. Blank and null records are treated as
     * error and emitted on error port with an error message. Header is also
     * treated as error and emitted on error port with error message
     */
    public void process(byte[] tuple)
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
        if (output.isConnected()) {
          csvStringReader.open(incomingString);
          Map<String, Object> map = csvMapReader.read(nameMapping, processors);
          output.emit(map);
          parsedRecords++;
        }

        if (pojo.isConnected() && clazz != null) {
          csvStringReader.open(incomingString);
          Object obj = csvBeanReader.read(clazz, nameMapping, processors);
          pojo.emit(obj);
          pojoRecords++;
        }

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
  };

  /**
   * Returns array of cellprocessors, one for each field
   */
  private CellProcessor[] getProcessor(List<Field> fields)
  {
    CellProcessor[] processor = new CellProcessor[fields.size()];
    int fieldCount = 0;
    for (Field field : fields) {
      processor[fieldCount++] = CellProcessorBuilder.getCellProcessor(field.getType(), field.getConstraints());
    }
    return processor;
  }

  @Override
  public void teardown()
  {
    try {
      csvMapReader.close();
    } catch (IOException e) {
      logger.error("Error while closing csv map reader {}", e.getMessage());
    }
    try {
      csvBeanReader.close();
    } catch (IOException e) {
      logger.error("Error while closing csv bean reader {}", e.getMessage());
    }
  }

  /**
   * Complete hdfs path of schema
   * 
   * @return
   */
  public String getSchemaPath()
  {
    return schemaPath;
  }

  /**
   * Set path of schema
   * 
   * @param schemaPath
   *          path of the schema file in hdfs
   */
  public void setSchemaPath(String schemaPath)
  {
    this.schemaPath = schemaPath;
    try {
      this.schema = FileUtils.readFromHDFS(schemaPath);
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

  /**
   * output port to emit validate records as pojos
   */
  public final transient DefaultOutputPort<Object> pojo = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  /**
   * error port to emit records that could not be processed
   */
  public final transient DefaultOutputPort<KeyValPair<String, String>> error = new DefaultOutputPort<KeyValPair<String, String>>();
  /**
   * output port to emit validate records as map
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();
  private static final Logger logger = LoggerFactory.getLogger(DelimitedParserOperator.class);

}
