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

import jline.internal.Log;

public class EmailNotification {
  private static final Logger logger = LoggerFactory.getLogger(EmailNotification.class);

  public void notify(EmailInfo emailInfo) {
    boolean enableAuthentication = (emailInfo.password != null);

    Properties props = new Properties();
    if (enableAuthentication)
      props.put("mail.smtp.auth", "true");
    if (emailInfo.enableTls)
      props.put("mail.smtp.starttls.enable", "true");
    props.put("mail.smtp.host", emailInfo.smtpServer);
    props.put("mail.smtp.port", emailInfo.smtpPort);

    final String sender = emailInfo.sender;
    final String password = String.valueOf(emailInfo.password);
    Session session = Session.getInstance(props, enableAuthentication ? new javax.mail.Authenticator() {
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(sender, password);
      }
    } : null);

    Message mimeMsg = new MimeMessage(session);
    try {
      mimeMsg.setFrom(new InternetAddress(emailInfo.sender));
    } catch (MessagingException ex) {
      logger.warn("Invalid Email sender", ex);
    }
    try {

      if (emailInfo.tos != null) {
        for (String to : emailInfo.tos) {
          mimeMsg.addRecipient(Message.RecipientType.TO, new InternetAddress(to.trim()));
        }
      }
      if (emailInfo.ccs != null) {
        for (String cc : emailInfo.ccs) {
          mimeMsg.addRecipient(Message.RecipientType.CC, new InternetAddress(cc.trim()));
        }
      }
      if (emailInfo.bccs != null) {
        for (String bcc : emailInfo.bccs) {
          mimeMsg.addRecipient(Message.RecipientType.BCC, new InternetAddress(bcc.trim()));
        }
      }
      mimeMsg.setSubject(emailInfo.subject);
      mimeMsg.setText(emailInfo.content);
      Transport.send(mimeMsg);
    } catch (MessagingException me) {
      Log.warn(me.getMessage());
    }
  }

}
