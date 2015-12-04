package com.datatorrent.modules.app.aggregation.regression;

public class POJO
{
  private int publisherId;
  private int advertiserId;
  private int locationId;
  private double cost;
  private int clicks;
  private long time;
  private String timeBucket;

  public POJO()
  {
  }

  public int getPublisherId()
  {
    return publisherId;
  }

  public void setPublisherId(int publisherId)
  {
    this.publisherId = publisherId;
  }

  public int getAdvertiserId()
  {
    return advertiserId;
  }

  public void setAdvertiserId(int advertiserId)
  {
    this.advertiserId = advertiserId;
  }

  public int getLocationId()
  {
    return locationId;
  }

  public void setLocationId(int locationId)
  {
    this.locationId = locationId;
  }

  public double getCost()
  {
    return cost;
  }

  public void setCost(double cost)
  {
    this.cost = cost;
  }

  public int getClicks()
  {
    return clicks;
  }

  public void setClicks(int clicks)
  {
    this.clicks = clicks;
  }

  public long getTime()
  {
    return time;
  }

  public void setTime(long time)
  {
    this.time = time;
  }

  public String getTimeBucket()
  {
    return timeBucket;
  }

  public void setTimeBucket(String timeBucket)
  {
    this.timeBucket = timeBucket;
  }

  @Override
  public String toString()
  {
    return "POJO{" +
      "publisherId=" + publisherId +
      ", advertiserId=" + advertiserId +
      ", locationId=" + locationId +
      ", cost=" + cost +
      ", clicks=" + clicks +
      ", time=" + time +
      ", timeBucket='" + timeBucket + '\'' +
      '}';
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

    POJO pojo = (POJO)o;

    if (publisherId != pojo.publisherId) {
      return false;
    }
    if (advertiserId != pojo.advertiserId) {
      return false;
    }
    if (locationId != pojo.locationId) {
      return false;
    }
    return !(timeBucket != null ? !timeBucket.equals(pojo.timeBucket) : pojo.timeBucket != null);

  }

  @Override
  public int hashCode()
  {
    int result = publisherId;
    result = 31 * result + advertiserId;
    result = 31 * result + locationId;
    result = 31 * result + (timeBucket != null ? timeBucket.hashCode() : 0);
    return result;
  }
}
