package com.datatorrent.alerts.notification.email;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This class all the information to send an email
 * @author bright
 *
 */
public class EmailInfo {
  protected String smtpServer;
  protected int smtpPort;
  protected String sender;
  protected char[] password;    //set password to null if support anonymous
  protected boolean enableTls;  //default is false
  
  protected Collection<String> tos;
  protected Collection<String> ccs;
  protected Collection<String> bccs;
  
  protected String subject;
  protected String content;
  
  public boolean isComplete()
  {
    return ( smtpServer != null && smtpPort != 0 && sender != null && ( tos != null && !tos.isEmpty() ) 
        && ( subject != null && !subject.isEmpty() ) && ( content != null && !content.isEmpty() ) );
  }
  
  @Override
  public EmailInfo clone()
  {
    EmailInfo newObj = new EmailInfo();
    newObj.smtpServer = smtpServer;
    newObj.smtpPort = smtpPort;
    newObj.sender = sender;
    if(password != null)
      newObj.password = Arrays.copyOf(password, password.length);
    if(tos != null)
      newObj.tos = new ArrayList<String>(tos);
    if(ccs != null)
      newObj.ccs = new ArrayList<String>(ccs);
    if(bccs != null)
      newObj.bccs = new ArrayList<String>(bccs);
    newObj.subject = subject;
    newObj.content = content;
    
    return newObj;
  }
  
  public EmailInfo mergeWith(EmailConf conf)
  {
    if(smtpServer==null || smtpServer.isEmpty())
    {
      smtpServer = conf.context.smtpServer;
      smtpPort = conf.context.smtpPort;
    }
    if( sender==null || sender.isEmpty() )
    {
      sender = conf.context.sender;
      password = conf.context.password;
    }
    if(tos==null||tos.isEmpty())
    {
      tos = conf.recipient.tos;
      ccs = conf.recipient.ccs;
      bccs = conf.recipient.bccs;
    }
    if(subject==null || subject.isEmpty())
      subject = conf.message.subject;
    if(content==null || content.isEmpty())
      content = conf.message.content;
    
    return this;
  }
}
