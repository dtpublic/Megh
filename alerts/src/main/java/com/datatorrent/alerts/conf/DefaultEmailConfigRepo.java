package com.datatorrent.alerts.conf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.datatorrent.alerts.notification.email.EmailContext;
import com.datatorrent.alerts.notification.email.EmailMessage;
import com.datatorrent.alerts.notification.email.EmailRecipient;
import com.datatorrent.certification.XmlConfig;
import com.datatorrent.certification.XmlConfig.BenchmarkGroup;
import com.datatorrent.certification.XmlConfig.Demo;
import com.datatorrent.certification.XmlConfig.Property;

import jline.internal.Log;

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
      Log.warn("Can NOT get alerts email configure file.");
      throw new AlertsConfigException("Can NOT get alerts email configure file.");
    }
    return filePath;
  }
  
  @Override
  public void loadConfig() {
    //read from hdfs 
    String fileName = getConfigFile();
    Path filePath = new Path(fileName);
    try {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream inputStream = fs.open(filePath);
      
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(inputStream);
      doc.getDocumentElement().normalize();
      
      //set support classes
      JAXBContext context = JAXBContext.newInstance(EmailContext.class, EmailMessage.class, EmailRecipient.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      
      //context;
      Map<String, EmailContext> contexts = new HashMap<String, EmailContext>();
      getElements(doc, unmarshaller, "emailContext", contexts);
      
      //messages;
      Map<String, EmailMessage> messages = new HashMap<String, EmailMessage>();
      getElements(doc, unmarshaller, "emailMessage", messages);
      
      //recipients;
      Map<String, EmailRecipient> recipients = new HashMap<String, EmailRecipient>();
      getElements(doc, unmarshaller, "emailRecipient", recipients);
      
      

      
    } catch (IOException | ParserConfigurationException | SAXException | JAXBException e) {
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

  
  protected <T> void getElements(Document doc, Unmarshaller unmarshaller, final String elementName, Map<String, T> elements) throws JAXBException
  {
    NodeList nodes = doc.getElementsByTagName(elementName);
    for(int index=0; index<nodes.getLength(); ++index)
    {
      String id = nodes.item(index).getAttributes().getNamedItem("id").getTextContent();
      elements.put(id, (T)unmarshaller.unmarshal(nodes.item(index)));
    }
  }
}
