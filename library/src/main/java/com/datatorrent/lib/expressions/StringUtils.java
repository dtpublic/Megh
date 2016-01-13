/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.expressions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils
{
  public static boolean equalsWithCase(String str1, String str2)
  {
    return org.apache.commons.lang3.StringUtils.equals(str1, str2);
  }

  public static String truncate(String str, int length)
  {
    return (str == null) ? null : str.substring(0, length);
  }

  public static String regexCapture(String str, String regex, int groupId)
  {
    if (groupId <= 0) {
      return null;
    }

    Pattern entry = Pattern.compile(regex);
    Matcher matcher = entry.matcher(str);

    return matcher.find() ? matcher.group(groupId) : null;
  }
}
