package com.datatorrent.alerts.conf;

import java.io.File;
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
      JAXBContext context = JAXBContext.newInstance(Conf.class, Conf.EmailContext.class, Conf.EmailMessage.class, Conf.EmailRecipient.class, Conf.Criterias.class);
      //JAXBContext context = JAXBContext.newInstance(EmailContextMutable.class, EmailMessageMutable.class, EmailRecipientMutable.class);
      
      Unmarshaller unmarshaller = context.createUnmarshaller();
      Conf conf = (Conf)unmarshaller.unmarshal(inputStream);
      logger.debug(conf.toString());
      
      if(conf != null)
        loadConfig(conf);

      
    } catch (Exception e) {
      logger.error("Get or parse configure file exception.", e);
    }
//    
//    {
//      Map<Integer, EmailContext> levelMap = new HashMap<Integer, EmailContext>();
//      levelMap.put(EmailConfigRepo.ANY_LEVEL, new EmailContext( "smtp.gmail.com", 587, "bright@datatorrent", "password".toCharArray(), true ) );
//      contextMap.put(EmailConfigRepo.ANY_APP, levelMap);
//    }
//    
//    {
//      Map<Integer, EmailMessage> levelMap = new HashMap<Integer, EmailMessage>();
//      levelMap.put(EmailConfigRepo.ANY_LEVEL, new EmailMessage( new String[]{""}, null, null, "notification", "content"  ) );
//      messageMap.put(EmailConfigRepo.ANY_APP, levelMap);
//    }
//    
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
      if(apps == null || apps.isEmpty())
      {
        app = ANY_APP;
      }
      String level = criteria.getL();
      if(level == null)
        level = ANY_LEVEL;
          
      
    }
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
    return new EmailRecipient(confRecipient.getTos().getTo(), confRecipient.getCcs().getCc(), confRecipient.getBccs().getBcc());
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
