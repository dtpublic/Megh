package com.datatorrent.alerts.notification.email;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.notification.Notification;

import jline.internal.Log;

public class EmailNotification implements Notification<EmailNotificationContext, EmailNotificationMessage> {
  private static final Logger logger = LoggerFactory.getLogger(EmailNotification.class);
  
  @Override
  public void notify(final EmailNotificationContext context, EmailNotificationMessage message) {
    boolean enableAuthentication = ( context.password != null );
    
    Properties props = new Properties();
    if(enableAuthentication)
      props.put("mail.smtp.auth", "true");
    if(context.enableTls)
      props.put("mail.smtp.starttls.enable", "true");
    props.put("mail.smtp.host", context.smtpServer);
    props.put("mail.smtp.port", context.smtpPort);

    Session session = Session.getInstance(props, 
        enableAuthentication ? 
       new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
              return new PasswordAuthentication(context.sender, String.valueOf(context.password));
            }
          } : null);

    Message mimeMsg = new MimeMessage(session);
    try {
      mimeMsg.setFrom(new InternetAddress(context.sender));
    } catch (MessagingException ex) {
      logger.warn("Invalid Email sender", ex);
    }
    try {
      for (String to : message.to) {
        mimeMsg.addRecipient(Message.RecipientType.TO, new InternetAddress(to.trim()));
      }
      if(message.cc != null )
      {
        for (String cc : message.cc) {
          mimeMsg.addRecipient(Message.RecipientType.CC, new InternetAddress(cc.trim()));
        }
      }
      if(message.bcc != null)
      {
        for (String bcc : message.bcc) {
          mimeMsg.addRecipient(Message.RecipientType.BCC, new InternetAddress(bcc.trim()));
        }
      }
      mimeMsg.setSubject(message.subject);
      mimeMsg.setText(message.content);
      Transport.send(mimeMsg);
    } catch (MessagingException me) {
      Log.warn(me.getMessage());
    }
  }

}
