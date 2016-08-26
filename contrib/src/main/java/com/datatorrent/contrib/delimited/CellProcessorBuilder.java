/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.delimited;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.DMinMax;
import org.supercsv.cellprocessor.constraint.Equals;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.constraint.Strlen;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;

import org.apache.commons.lang3.StringUtils;

/**
 * Helper class with methods to generate CellProcessor objects. Cell processors
 * are an integral part of reading and writing with Super CSV - they automate
 * the data type conversions, and enforce constraints. They implement the chain
 * of responsibility design pattern - each processor has a single, well-defined
 * purpose and can be chained together with other processors to fully automate
 * all of the required conversions and constraint validation for a single
 * delimited record. Users of CellProcessor are concrete implementations of
 * {@link AbstractDelimitedProcessorOperator}
 * 
 */
public class CellProcessorBuilder
{

  /**
   * Method to get cellprocessor for String with constraints. These constraints
   * are evaluated against the String field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param strLen
   *          length of the field.
   * @param minLength
   *          minimum length of the field.
   * @param maxLength
   *          maximum length of the field.
   * @param equals
   *          field must be equal to this value.
   * @param pattern
   *          field must match the provided regex pattern.
   * @return CellProcessor
   */
  public static CellProcessor getStringCellProcessor(Boolean required, Integer strLen, Integer minLength,
      Integer maxLength, String equals, String pattern)
  {
    CellProcessor cellProcessor = null;
    if (StringUtils.isNotBlank(pattern)) {
      cellProcessor = addRegex(pattern);
    } else if (maxLength != null || minLength != null) {
      cellProcessor = addStrMinMax(cellProcessor, minLength, maxLength);
    } else if (strLen != null) {
      cellProcessor = addStrLength(cellProcessor, strLen);
    } else if (StringUtils.isNotBlank(equals)) {
      cellProcessor = addEquals(cellProcessor, equals);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get basic StringCellProcessor that indicates that no processing
   * is required and String is used unchanged.
   * 
   * @return CellProcessor
   */
  public static CellProcessor getStringCellProcessor()
  {
    return new Optional();
  }

  /**
   * Method to get cellprocessor for Integer with constraints. These constraints
   * are evaluated against the integer field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param equals
   *          field must be equal to this value.
   * @return CellProcessor
   */
  public static CellProcessor getIntegerCellProcessor(Boolean required, Integer equals)
  {
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = addEquals(cellProcessor, equals);
    }
    cellProcessor = addParseInt(cellProcessor);
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Get basic Integer cellprocessor to convert String to Integer.
   * 
   * @return CellProcessor
   */
  public static CellProcessor getIntegerCellProcessor()
  {
    return new Optional(new ParseInt());
  }

  /**
   * Method to get cellprocessor for Long with constraints. These constraints
   * are evaluated against the Long field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param equals
   *          field must be equal to this value.
   * @param minValue
   *          minimum value of the field.
   * @param maxValue
   *          maximum value of the field.
   * @return CellProcessor
   */
  public static CellProcessor getLongCellProcessor(Boolean required, Long equals, Long minValue, Long maxValue)
  {
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = addEquals(cellProcessor, equals);
      cellProcessor = addParseLong(cellProcessor);
    } else if (minValue != null || maxValue != null) {
      cellProcessor = addLongMinMax(minValue, maxValue);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Get basic Long cellprocessor to convert String to Long.
   * 
   * @return CellProcessor
   */
  public static CellProcessor getLongCellProcessor()
  {
    return new Optional(new ParseLong());
  }

  /**
   * Method to get cellprocessor for Double with constraints. These constraints
   * are evaluated against the Double field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param equals
   *          field must be equal to this value.
   * @param minValue
   *          minimum value of the field.
   * @param maxValue
   *          maximum value of the field.
   * @return CellProcessor
   */
  public static CellProcessor getDoubleCellProcessor(Boolean required, Double equals, Double minValue, Double maxValue)
  {
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = addEquals(cellProcessor, equals);
      cellProcessor = addParseDouble(cellProcessor);
    } else if (minValue != null || maxValue != null) {
      cellProcessor = addDoubleMinMax(minValue, maxValue);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Get basic Double cellprocessor to convert String to Double.
   * 
   * @return CellProcessor
   */
  public static CellProcessor getDoubleCellProcessor()
  {
    return new Optional(new ParseDouble());
  }

  /**
   * Method to get cellprocessor for Boolean with constraints. These constraints
   * are evaluated against the Boolean field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param trueValue
   *          converts string to boolean with supplied true value.
   * @param falseValue
   *          converts string to boolean with supplied false value.
   * @return CellProcessor
   */
  public static CellProcessor getBooleanCellProcessor(Boolean required, String trueValue, String falseValue)
  {
    CellProcessor cellProcessor = null;
    if (StringUtils.isNotBlank(trueValue) && StringUtils.isNotBlank(falseValue)) {
      cellProcessor = new ParseBool(trueValue, falseValue);
    } else {
      cellProcessor = new ParseBool();
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Date with constraints. These constraints
   * are evaluated against the Date field for which this cellprocessor is
   * defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param format
   *          Converts a String to a Date using the SimpleDateFormat class
   *          default being dd/MM/yyyy.
   * @return Cellprocessor
   */
  public static CellProcessor getDateCellProcessor(Boolean required, String format)
  {
    CellProcessor cellProcessor = null;
    String fmt = StringUtils.isNotBlank(format) ? format : "dd/MM/yyyy";
    cellProcessor = new ParseDate(fmt, false);
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Character with constraints. These
   * constraints are evaluated against the Character field for which this
   * cellprocessor is defined.
   * 
   * @param required
   *          if true this field is mandatory.
   * @param equals
   *          field must be equal to this value.
   * @return CellProcessor
   */
  public static CellProcessor getCharCellProcessor(Boolean required, Character equals)
  {
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = addEquals(cellProcessor, equals);
    }
    cellProcessor = addParseChar(cellProcessor);
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Get Char cellprocessor.
   * 
   * @return CellProcessor
   */
  public static CellProcessor getCharCellProcessor()
  {
    return new Optional(new ParseChar());
  }

  /**
   * Get a Double Min Max cellprocessor.
   * 
   * @param minValue
   *          minimum value.
   * @param maxValue
   *          maximum value.
   * @return CellProcessor
   */
  private static CellProcessor addDoubleMinMax(Double minValue, Double maxValue)
  {
    Double min = minValue == null ? DMinMax.MIN_DOUBLE : minValue;
    Double max = maxValue == null ? DMinMax.MAX_DOUBLE : maxValue;
    return new DMinMax(min, max);
  }

  /**
   * Get a Long Min Max cellprocessor.
   * 
   * @param minValue
   *          minimum value.
   * @param maxValue
   *          maximum value.
   * @return CellProcessor
   */
  private static CellProcessor addLongMinMax(Long minValue, Long maxValue)
  {
    Long min = minValue == null ? LMinMax.MIN_LONG : minValue;
    Long max = maxValue == null ? LMinMax.MAX_LONG : maxValue;
    return new LMinMax(min, max);
  }

  /**
   * Get a String Length cellprocessor.
   * 
   * @param cellProcessor
   *          next cellprocessor in the chain.
   * @param strLen
   *          length of string.
   * @return CellProcessor
   */
  private static CellProcessor addStrLength(CellProcessor cellProcessor, int strLen)
  {
    if (cellProcessor == null) {
      return new Strlen(strLen);
    }
    return new Strlen(strLen, cellProcessor);
  }

  /**
   * Get Equals cellprocessor.
   * 
   * @param cellProcessor
   *          next cellprocessor in the chain.
   * @param obj
   *          the constant value that input must be equal to.
   * @return CellProcessor
   */
  private static CellProcessor addEquals(CellProcessor cellProcessor, Object obj)
  {
    if (cellProcessor == null) {
      return new Equals(obj);
    }
    return new Equals(obj, cellProcessor);
  }

  /**
   * Get String Minlength Maxlength cellprocessor.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @param minLength
   *          the minimum length of the string.
   * @param maxLength
   *          the maximum length of the string.
   * @return CellProcessor
   */
  private static CellProcessor addStrMinMax(CellProcessor cellProcessor, Integer minLength, Integer maxLength)
  {
    Long min = minLength == null ? 0L : minLength;
    Long max = maxLength == null ? LMinMax.MAX_LONG : maxLength;

    if (cellProcessor == null) {
      return new StrMinMax(min, max);
    }
    return new StrMinMax(min, max, cellProcessor);
  }

  /**
   * Get Optional cellprocessor which means field is not mandatory.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addOptional(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new Optional();
    }
    return new Optional(cellProcessor);
  }

  /**
   * Get Regex cellprocessor.
   * 
   * @param pattern
   *          regular expression to match.
   * @return CellProcessor
   */
  private static CellProcessor addRegex(String pattern)
  {
    return new StrRegEx(pattern);
  }

  /**
   * Get cellprocessor to parse String as Integer.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseInt(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseInt();
    }
    return new ParseInt((LongCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Long.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseLong(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseLong();
    }
    return new ParseLong((LongCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Double.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseDouble(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseDouble();
    }
    return new ParseDouble((DoubleCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Character.
   * 
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseChar(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseChar();
    }
    return new ParseChar((DoubleCellProcessor)cellProcessor);
  }

}
