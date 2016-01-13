/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.expressions;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTimeUtils
{
  public Date nowDate()
  {
    return new Date();
  }

  public long nowTime()
  {
    return System.currentTimeMillis();
  }

  public Calendar nowCalender()
  {
    return Calendar.getInstance();
  }

  public Calendar nowCalender(String timeZone)
  {
    return Calendar.getInstance(TimeZone.getTimeZone(timeZone));
  }
}
