/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.schema;

import java.util.Map;

import com.google.common.collect.Maps;

public enum TypeInfo
{
  BOOLEAN("boolean", boolean.class, Boolean.class),
  BYTE("byte", byte.class, Byte.class),
  CHAR("char", char.class, Character.class),
  SHORT("short", short.class, Short.class),
  INT("integer", int.class, Integer.class),
  LONG("long", long.class, Long.class),
  FLOAT("float", float.class, Float.class),
  DOUBLE("double", double.class, Double.class),
  STRING("string", String.class, String.class);

  private final String name;
  private final Class typeClass;
  private final Class wrapperClass;

  TypeInfo(String name, Class klass, Class wrapperClass)
  {
    this.name = name;
    this.typeClass = klass;
    this.wrapperClass = wrapperClass;
  }

  public String getName()
  {
    return name;
  }

  public Class getTypeClass()
  {
    return typeClass;
  }

  public Class getWrapperClass()
  {
    return wrapperClass;
  }

  static final TypeInfo[] supportedTypes = new TypeInfo[]{BOOLEAN, BYTE, CHAR, SHORT, INT, LONG, FLOAT, DOUBLE, STRING};

  static final Map<String, TypeInfo> nameToTypeMap = Maps.newHashMap();
  static final Map<Class, TypeInfo> classToTypeMap = Maps.newHashMap();
  static final Map<Class, String> classToNameMap = Maps.newHashMap();

  static {
    for (TypeInfo ti : supportedTypes) {
      nameToTypeMap.put(ti.name, ti);

      classToTypeMap.put(ti.typeClass, ti);
      classToTypeMap.put(ti.wrapperClass, ti);

      classToNameMap.put(ti.typeClass, ti.name);
      classToNameMap.put(ti.wrapperClass, ti.name);
    }
  }

  public static String getNameFromType(Class type)
  {
    return classToNameMap.get(type);
  }

  public static TypeInfo getTypeInfo(String name)
  {
    return nameToTypeMap.get(name);
  }

  public static boolean isTypeSupported(Class klass)
  {
    return classToNameMap.containsKey(klass);
  }

  public static TypeInfo getTypeInfoFromClass(Class<?> type)
  {
    return classToTypeMap.get(type);
  }
}
