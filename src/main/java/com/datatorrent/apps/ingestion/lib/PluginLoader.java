package com.datatorrent.apps.ingestion.lib;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.tools.ant.DirectoryScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>PluginLoader class.</p>
 *
 * @since 1.0.0
 */
public class PluginLoader
{
  private static Logger LOG = LoggerFactory.getLogger(PluginLoader.class);
  private static final String DT_PLUGINS_DIR = "/.dt/plugins";

  /**
   * Searches datatorrent plugins directory for plugin which implements superInterface and of given type and name
   * @param type type of the plugin e.g. compression
   * @param name name of plugin e.g. lzo
   *
   * @return class of given type and name which implements superInterface from discovered plugins
   */
  public static PluginInfo discoverPlugin(String type, String name)
  {
    List<String> jarPaths = getJarsToScan();
    for (String jarPath : jarPaths) {
      try {
        Manifest mf = new JarFile(jarPath).getManifest();
        Attributes manifestAttrs = mf.getMainAttributes();
        String pluginType = manifestAttrs.getValue("PluginType");
        String pluginName = manifestAttrs.getValue("PluginName");
        if (pluginType != null && "compression".equals(pluginType) && pluginName != null && "lzo".equals(pluginName)) {
          PluginInfo pluginInfo = new PluginInfo(jarPath, manifestAttrs.getValue("PluginClass"));
          return pluginInfo;
        }
      } catch (IOException e) {
        LOG.error("Plugin discovery failed to load jar: " + jarPath);
      }
    }
    return null;
  }

  private static List<String> getJarsToScan()
  {
    String userHome = System.getProperty("user.home");
    File dir = new File(userHome + DT_PLUGINS_DIR);
    if (!dir.exists()) {
      System.err.println("Plugins directory does not exits.");
      return Collections.emptyList();
    }
    List<String> result = new LinkedList<String>();
    DirectoryScanner ds = new DirectoryScanner();
    ds.setBasedir(dir.getPath());
    ds.scan();
    for (String libJar : ds.getIncludedFiles()) {
      if (libJar.endsWith(".jar")) {
        result.add(dir + File.separator + libJar);
      }
    }
    return result;
  }

  public static class PluginInfo
  {
    private String jarName;
    private String className;

    public PluginInfo(String jarName, String className)
    {
      this.jarName = jarName;
      this.className = className;
    }

    public String getClassName()
    {
      return className;
    }

    public String getJarName()
    {
      return jarName;
    }
  }
}
