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

import com.datatorrent.alerts.conf.xmlbind.Conf;
import com.datatorrent.alerts.notification.email.EmailContext;
import com.datatorrent.alerts.notification.email.EmailMessage;
import com.datatorrent.alerts.notification.email.EmailRecipient;

public class DefaultEmailConfigRepo extends EmailConfigRepo {
  protected static class EmailConfMutable
  {
    protected EmailContext context;
    protected EmailRecipient recipient;
    protected EmailMessage message;
  }
  
  private static final Logger logger = LoggerFactory.getLogger(DefaultEmailConfigRepo.class);
      
  private final String PROP_ALERTS_EMAIL_CONF_FILE = "alerts.email.conf.file";
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
      
//      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//      DocumentBuilder builder = factory.newDocumentBuilder();
//      Document doc = builder.parse(inputStream);
//      doc.getDocumentElement().normalize();
      
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
    
    Map<EmailConfigCondition, EmailConfMutable> emailConfMutableMap = new HashMap<EmailConfigCondition, EmailConfMutable>();
    for(Conf.Criteria criteria : criterias)
    {
      List<String> apps = criteria.getApp();
      List<Integer> levels = criteria.getLevel();
      
      List<EmailConfigCondition> conditions = getConditions( apps, levels );
      if( conditions != null )
      {
        for( EmailConfigCondition condition : conditions )
        {
          EmailContext context = null;
          if( contextMap != null && !contextMap.isEmpty() && criteria.getEmailContextRef() != null )
            context = contextMap.get(criteria.getEmailContextRef());
          EmailRecipient recipient = null;
          if( recipientMap != null && !recipientMap.isEmpty() && criteria.getEmailRecipientRef() !=null )
            recipient = recipientMap.get(criteria.getEmailRecipientRef());
          EmailMessage message = null;
          if( messageMap != null && !messageMap.isEmpty() && criteria.getEmailMessageRef() !=null )
            message = messageMap.get(criteria.getEmailMessageRef());
          
          mergeConfig( emailConfMutableMap, condition, context, recipient, message);
        }
      }
    }
  }
  
  protected static void mergeConfig(Map<EmailConfigCondition, EmailConfMutable> emailConfMutableMap, EmailConfigCondition condition,
      EmailContext context, EmailRecipient recipient, EmailMessage message )
  {

    EmailConfMutable conf = emailConfMutableMap.get(condition);
    if(conf == null)
    {
      conf = new EmailConfMutable();
      emailConfMutableMap.put(condition, conf);
    }
    // merge in fact is override
    conf.context = context;
    conf.recipient = recipient;
    conf.message = message;
  }
  
  protected static List<EmailConfigCondition> getConditions( List<String> apps, List<Integer> levels )
  {
    if( (apps == null || apps.isEmpty()) && (levels == null || levels.isEmpty()) )
      return null;
    
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
        confContext.getPassword().toCharArray(), confContext.isEnableTls() );
  }
  
  protected static EmailMessage getEmailMessage(Conf.EmailMessage confMessage)
  {
    return new EmailMessage(confMessage.getSubject(), confMessage.getContent());
  }
  
  protected static EmailRecipient getEmailRecipient(Conf.EmailRecipient confRecipient)
  {
    return new EmailRecipient(confRecipient.getTo(), confRecipient.getCc(), confRecipient.getBcc());
  }
//  
//  protected <T, M> void getElements(Document doc, Unmarshaller unmarshaller, final String elementName, Map<String, T> elements, 
//      Class<T> immutableClass, Class<M> mutableClass  ) throws JAXBException
//  {
//    NodeList nodes = doc.getElementsByTagName(elementName);
//    for(int index=0; index<nodes.getLength(); ++index)
//    {
//      String id = nodes.item(index).getAttributes().getNamedItem("id").getTextContent();
//      M mutable = (M)unmarshaller.unmarshal(nodes.item(index));
//      T immutable = null;
//      try {
//        immutable = immutableClass.getConstructor(mutableClass).newInstance(mutable);
//      } catch (Exception e) {
//        logger.error("Try to create instance failed.", e);
//        throw new AlertsConfigException("create instance exception.");
//      }
//      elements.put(id, immutable);
//    }
//  }
}
