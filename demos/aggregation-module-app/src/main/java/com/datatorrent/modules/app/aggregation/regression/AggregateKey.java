/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation.regression;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

public class AggregateKey
{
  private List<String> combination;
  private POJO pojo;

  public AggregateKey(List<String> combination, POJO pojo)
  {
    this.combination = combination;
    this.pojo = pojo;
    Collections.sort(this.combination);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AggregateKey that = (AggregateKey)o;

    if (combination != null ? !combination.equals(that.combination) : that.combination != null) {
      return false;
    }

    // Check if those fields if POJO which are present in combination same.
    for (String field : combination) {
      try {
        Field declaredField = pojo.getClass().getDeclaredField(field);
        declaredField.setAccessible(true);
        Object thisObject = declaredField.get(this.pojo);
        Object thatObject = declaredField.get(that.pojo);
        if (!thisObject.equals(thatObject)) {
          return false;
        }
      } catch (NoSuchFieldException e) {
        e.printStackTrace();
        return false;
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = combination != null ? combination.hashCode() : 0;
    for (String field : combination) {
      try {
        Field declaredField = pojo.getClass().getDeclaredField(field);
        declaredField.setAccessible(true);
        result = 31 * result + declaredField.get(this.pojo).hashCode();
      } catch (NoSuchFieldException e) {
        e.printStackTrace();
        return 0;
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        return 0;
      }
    }

    return result;
  }

  @Override
  public String toString()
  {
    return "AggregateKey{" +
      "combination=" + combination +
      ", pojo=" + pojo +
      '}';
  }
}
