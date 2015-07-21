package com.datatorrent.lib.bucket;

/**
 * Events having a expiry field. <br/>
 * This field can be used to expire incoming tuples based some function of expiry key of the event
 */
public interface Expirable
{
  Object getExpiryKey();
}
