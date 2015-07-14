package com.datatorrent.malhar.lib.io.db;

import java.io.IOException;

/**
 * This interface is for any object that needs to be connected to do operations
 *
 * @since 0.9.3
 */
public interface Connectable
{
  /**
   * Connects to the service.
   *
   * @throws java.io.IOException
   */
  public void connect() throws IOException;

  /**
   * Disconnects from the service.
   *
   * @throws IOException
   */
  public void disconnect() throws IOException;

  /**
   *
   * @return returns whether the service is connected.
   */
  public boolean isConnected();
}
