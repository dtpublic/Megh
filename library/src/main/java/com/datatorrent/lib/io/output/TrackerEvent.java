/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

/**
 * <p>TrackerEvent class.</p>
 *
 * @since 1.0.0
 */
public class TrackerEvent
{
  public static enum TrackerEventType {
    SUCCESSFUL_FILE, FAILED_FILE, SKIPPED_FILE, INFO, DISCOVERED
  }
  
  public interface TrackerEventDetails{
    public String getDescription();
  }
  
  public static class StringTrackerEventDetails implements TrackerEventDetails{
    String description;
    
    public StringTrackerEventDetails()
    {
      // For kryo
    }
    
    public StringTrackerEventDetails(String description)
    {
      this.description = description;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String getDescription()
    {
      return description;
    }
  }

  private TrackerEventType type;
  private TrackerEventDetails details;

  public TrackerEvent(TrackerEventType type, TrackerEventDetails details)
  {
    this.type = type;
    this.details = details;
  }
  
  public TrackerEvent(TrackerEventType type, String description)
  {
    this(type,new StringTrackerEventDetails(description));
  }
  
  public TrackerEvent()
  {
    // For kryo
  }

  public TrackerEventType getType()
  {
    return type;
  }

  public void setType(TrackerEventType type)
  {
    this.type = type;
  }

  public TrackerEventDetails getDetails()
  {
    return details;
  }
  
  public void setDetails(TrackerEventDetails details)
  {
    this.details = details;
  }
}
