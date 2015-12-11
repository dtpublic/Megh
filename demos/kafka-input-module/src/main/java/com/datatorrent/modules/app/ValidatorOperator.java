package com.datatorrent.modules.app;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class ValidatorOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(ValidatorOperator.class);
  private transient MessageDigest lmd , rmd;
  private long windowId;
  private int leftCount, rightCount;
  protected transient FileSystem fs;
  private static final String statusFile = "Error.log";
  private transient FSDataOutputStream out;
  private transient ArrayBlockingQueue<byte[]> lqueue,rqueue;
  private int count =0;
  @Min(1)
  private int maxTuplesPerWindow = 1;
  private boolean disableValidation = false;

  @Override
  public void setup(Context.OperatorContext context)
  {
    Path statusPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + statusFile);
    Configuration configuration = new Configuration();

    try {
      fs = FileSystem.newInstance(statusPath.toUri(), configuration);
      out = fs.create(statusPath, true);
      lmd = MessageDigest.getInstance("MD5");
      rmd = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MessageDigest has no Such Algorithm: " + e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create File: " + statusPath.getName());
    }

    leftCount = 0;
    rightCount = 0;
    count = 0;
    lqueue = new ArrayBlockingQueue<>(maxTuplesPerWindow);
    rqueue = new ArrayBlockingQueue<>(maxTuplesPerWindow);
  }

  @Override
  public void teardown()
  {
    try {
      if(fs != null) {
        fs.close();
      }
      if(out != null) {
        out.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to close stream " + e);
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.count = 0;
    this.windowId = windowId;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    //Check if the leftCount and rightCount reaches maxTuplesPerWindow
    if(leftCount == maxTuplesPerWindow && rightCount == maxTuplesPerWindow) {
      //Verify the left and right checksum are equal
      byte[] leftBytes = lmd.digest();
      byte[] rightBytes = rmd.digest();
      if(!MessageDigest.isEqual(leftBytes, rightBytes)) {
        try {
          out.writeBytes("Error: Kafka and RandomGenerator data are not equal");
          out.flush();
        } catch (IOException e) {
          throw new RuntimeException("Unable to write " + e);
        }
      }
      //Reset the digests and update it with the elements of queue
      lmd.reset();
      rmd.reset();
      leftCount = lqueue.size();
      rightCount = rqueue.size();
      if(lqueue.size() != 0) {
        int size = lqueue.size();
        for(int i = 0; i < size; i++) {
          byte[] x = lqueue.poll();
          lmd.update(x);
        }
      }
      if(rqueue.size() != 0) {
        int size = rqueue.size();
        for(int i = 0; i < size; i++) {
          byte[] x = rqueue.poll();
          rmd.update(x);
        }
      }
    }
  }

  public final transient DefaultInputPort<Integer> randomPort = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer s)
    {
      if(leftCount < maxTuplesPerWindow) {
        lmd.update(BigInteger.valueOf(s).toByteArray());
        leftCount++;
      } else {
        lqueue.add(BigInteger.valueOf(s).toByteArray());
      }
    }
  };

  public final transient DefaultInputPort<byte[]> kafkaPort = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] s)
    {
      if(disableValidation) {
        logger.info(new String(s));
        return;
      }
      if(rightCount < maxTuplesPerWindow) {
        rmd.update(s);
        rightCount++;
      } else {
        rqueue.add(s);
      }
      count++;
      if(count > maxTuplesPerWindow) {
        try {
          out.writeBytes("Error: Number of tuples Emitting from Kafka exceeds the maxTuplesPerWindow : {}" + windowId);
          out.flush();
        } catch (IOException e) {
          throw new RuntimeException("Unable to write " + e);
        }
      }
    }
  };

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public boolean isDisableValidation()
  {
    return disableValidation;
  }

  public void setDisableValidation(boolean disableValidation)
  {
    this.disableValidation = disableValidation;
  }
}
