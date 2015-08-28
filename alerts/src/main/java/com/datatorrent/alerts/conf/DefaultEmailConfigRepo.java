package com.datatorrent.alerts.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.alerts.conf.EmailConfigRepo.EmailConfigCondition;
import com.datatorrent.alerts.conf.xmlbind.Conf;
import com.datatorrent.alerts.notification.email.EmailConf;
import com.datatorrent.alerts.notification.email.EmailContext;
import com.datatorrent.alerts.notification.email.EmailMessage;
import com.datatorrent.alerts.notification.email.EmailRecipient;
import com.google.common.collect.Lists;

public class DefaultEmailConfigRepo extends EmailConfigRepo {

  private static final Logger logger = LoggerFactory.getLogger(DefaultEmailConfigRepo.class);
      
  public static final String PROP_ALERTS_EMAIL_CONF_FILE = "alerts.email.conf.file";
  private static DefaultEmailConfigRepo instance = null;
  
  private final Configuration conf = new Configuration();
  
  public static DefaultEmailConfigRepo instance()
  {
    if( instance == null )
    {
      synchronized(DefaultEmailConfigRepo.class)
      {
        if( instance == null )
        {
          instance = new DefaultEmailConfigRepo();
          instance.loadConfig();
        }
      }
    }
    return instance;
  }
  
  protected String getConfigFile()
  {
    //get from system properties first
    String filePath = System.getProperty(PROP_ALERTS_EMAIL_CONF_FILE);
    if( filePath == null || filePath.isEmpty() )
    {
      //get from alerts config file
      filePath = AlertsProperties.instance().getProperty(PROP_ALERTS_EMAIL_CONF_FILE);
    }
    if( filePath == null || filePath.isEmpty() )
    {
      logger.info("Can not get email configure file informaiton from system property and alerts configuration. use default.");
      filePath = "EmailNotificationConf.xml";
    }
    return filePath;
  }
  
  @Override
  public void loadConfig() {
    //read from hdfs 
    String fileName = getConfigFile();
    File file = new File(fileName);
    logger.info("absolute file path: {}", file.getAbsolutePath() );
    logger.info("Email configure file path: {}", fileName);
    Path filePath = new Path(fileName);
    try {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream inputStream = fs.open(filePath);
      
      //set support classes
      JAXBContext context = JAXBContext.newInstance(Conf.class, Conf.EmailContext.class, Conf.EmailMessage.class, Conf.EmailRecipient.class, Conf.Criteria.class);
      //JAXBContext context = JAXBContext.newInstance(EmailContextMutable.class, EmailMessageMutable.class, EmailRecipientMutable.class);
      
      Unmarshaller unmarshaller = context.createUnmarshaller();
      Conf conf = (Conf)unmarshaller.unmarshal(inputStream);
      logger.debug(conf.toString());
      
      if(conf != null)
        loadConfig(conf);

      
    } catch (Exception e) {
      logger.error("Get or parse configure file exception.", e);
    }
    logger.info("Load configuration done.");
  }

  protected void loadConfig(Conf conf)
  {
    List<Conf.Criteria> criterias = conf.getCriteria();
    if( criterias == null || criterias.isEmpty() )
      return;
    
    Map<Integer, EmailContext> contextMap = new HashMap<Integer, EmailContext>();
    {
      List<Conf.EmailContext> contexts = conf.getEmailContext();
      if(contexts != null)
      {
        for(Conf.EmailContext context : contexts )
        {
          contextMap.put(context.getId(), getEmailContext(context) );
        }
      }
    }
    
    Map<Integer, EmailMessage> messageMap = new HashMap<Integer, EmailMessage>();
    {
      List<Conf.EmailMessage> messages = conf.getEmailMessage();
      if(messages != null)
      {
        for(Conf.EmailMessage message : messages )
        {
          messageMap.put(message.getId(), getEmailMessage(message) );
        }
      }
    }
    
    Map<Integer, EmailRecipient> recipientMap = new HashMap<Integer, EmailRecipient>();
    {
      List<Conf.EmailRecipient> recipients = conf.getEmailRecipient();
      if(recipients != null)
      {
        for(Conf.EmailRecipient recipient : recipients )
        {
          recipientMap.put(recipient.getId(), getEmailRecipient(recipient) );
        }
      }
    }
    
    for(Conf.Criteria criteria : criterias)
    {
      List<String> apps = criteria.getApp();
      List<Integer> levels = criteria.getLevel();
      
      List<EmailConfigCondition> conditions = getConditions( apps, levels );

      for( EmailConfigCondition condition : conditions )
      {
        EmailContext context = null;
        if( contextMap != null && !contextMap.isEmpty() && criteria.getEmailContextRef() != null )
          context = contextMap.get(criteria.getEmailContextRef());
        
        //it is possible there are multiple recipient for each criteria, merge them
        EmailRecipient recipient = null;
        if( recipientMap != null && !recipientMap.isEmpty() && criteria.getEmailRecipientRef() !=null )
        {
          List<EmailRecipient> subRecipients = Lists.newArrayList();
          for(Integer recipientRef : criteria.getEmailRecipientRef())
          {
            subRecipients.add(recipientMap.get(recipientRef));
          }
          recipient = EmailRecipient.mergeAll(subRecipients);
        }
        EmailMessage message = null;
        if( messageMap != null && !messageMap.isEmpty() && criteria.getEmailMessageRef() !=null )
          message = messageMap.get(criteria.getEmailMessageRef());
        
        mergeConfig( emailConfMap, condition, context, recipient, message);
      }
    }
    
    dumpEmailConf();
  }
  
  public void dumpEmailConf()
  {
    if(emailConfMap == null || emailConfMap.isEmpty())
      logger.info("email config is empty.");
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<EmailConfigCondition, EmailConf> entry : emailConfMap.entrySet())
    {
      sb.append(String.format("(%s) ==> (%s)\n", entry.getKey(), entry.getValue()));
    }
    logger.info("email configure:\n{}\n\n", sb); 
  }
 
  protected static void mergeConfig(Map<EmailConfigCondition, EmailConf> emailConfMap, EmailConfigCondition condition,
      EmailContext context, EmailRecipient recipient, EmailMessage message )
  {

    EmailConf conf = emailConfMap.get(condition);
    if(conf == null)
    {
      conf = new EmailConf();
      emailConfMap.put(condition, conf);
    }
    // merge in fact is override
    conf.setValue(context, recipient, message);
  }
  
  protected static List<EmailConfigCondition> getConditions( List<String> apps, List<Integer> levels )
  {
    if( (apps == null || apps.isEmpty()) && (levels == null || levels.isEmpty()) )
      return Lists.newArrayList(EmailConfigCondition.DEFAULT);
    
    List<EmailConfigCondition> conditions = new ArrayList<EmailConfigCondition>();
    if(apps == null || apps.isEmpty())
    {
      for( Integer level : levels )
        conditions.add( new EmailConfigCondition(level) );
      return conditions;
    }
    if(levels == null || levels.isEmpty())
    {
      for( String app : apps )
        conditions.add( new EmailConfigCondition(app) );
      return conditions;
    }
    for(String app : apps)
    {
      for(Integer level : levels)
        conditions.add( new EmailConfigCondition(app, level) );
    }
    return conditions;
  }
  
  protected static EmailContext getEmailContext(Conf.EmailContext confContext)
  {
    return new EmailContext(confContext.getSmtpServer(), confContext.getSmtpPort(), confContext.getSender(), 
        confContext.getPassword()==null ? null : confContext.getPassword().toCharArray(), confContext.isEnableTls() );
  }
  
  protected static EmailMessage getEmailMessage(Conf.EmailMessage confMessage)
  {
    return new EmailMessage(confMessage.getSubject(), confMessage.getContent());
  }
  
  protected static EmailRecipient getEmailRecipient(Conf.EmailRecipient confRecipient)
  {
    return new EmailRecipient(confRecipient.getTo(), confRecipient.getCc(), confRecipient.getBcc());
  }
}
