package com.datatorrent.alerts;

import java.net.SocketAddress;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * @since 2.1.0
 */
public class Message implements Comparator<Message>, Comparable<Message>
{
  private String message;
  private SocketAddress socketAddress;
  private Integer id ;
  private Integer level ;
  private Date lastLevelChange ;
  private List<String> messageReceivers ;
  private boolean flag ;

  public SocketAddress getSocketAddress() {
    return socketAddress;
  }

  public void setSocketAddress(SocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getLevel() {
    return level;
  }

  public void setLevel(Integer level) {
    this.level = level;
  }

  public Date getLastLevelChange() {
    return lastLevelChange;
  }

  public void setLastLevelChange(Date lastLevelChange) {
    this.lastLevelChange = lastLevelChange;
  }

  public List<String> getMessageReceivers() {
    return messageReceivers;
  }

  public void setMessageReceivers(List<String> messageReceivers) {
    this.messageReceivers = messageReceivers;
  }

  public boolean isFlag() {
    return flag;
  }

  public void setFlag(boolean flag) {
    this.flag = flag;
  }

  @Override
  public int compare(Message message, Message t1) {
    Long diff = message.getLastLevelChange().getTime() - t1.getLastLevelChange().getTime() ;

    if ( diff < 0 ) return -1 ;
    if ( diff == 0 ) return 0 ;
    return 1 ;
  }

  @Override
  public boolean equals (Object obj) {

    if (!(obj instanceof Message))
      return false;

    if (obj == this)
      return true;

    if ( this.getId() == ((Message) obj).getId()) {
      return true ;
    }

    return false ;
  }

  @Override
  public int hashCode() {
    return getId() ;
  }

  @Override
  public int compareTo(Message message) {
    return compare ( this , message) ;
  }
}