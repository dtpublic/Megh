package com.datatorrent.apps.ingestion.io.output;

import com.datatorrent.api.Context;
import com.datatorrent.contrib.splunk.SplunkStore;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.splunk.TcpInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Concrete class of Splunk bytes output operator which extends from AbstractStoreOutputOperator
 */
public class SplunkBytesOutputOperator extends AbstractStoreOutputOperator<byte[], SplunkStore>
{
  private String tcpPort;
  private transient Socket socket;
  private transient TcpInput tcpInput;
  private transient DataOutputStream stream;

  public String getTcpPort() {

    return tcpPort;
  }
  public void setTcpPort(String tcpPort) {

    this.tcpPort = tcpPort;
  }

  @Override
  public void setup(Context.OperatorContext context) {

    super.setup(context);
    tcpInput = (TcpInput) store.getService().getInputs().get(tcpPort);
    try {
      socket = tcpInput.attach();
      stream = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processTuple(byte[] tuple) {

    try {
      stream.write(tuple);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void endWindow() {

    try {
      stream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown() {

    super.teardown();
    try {
      stream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
