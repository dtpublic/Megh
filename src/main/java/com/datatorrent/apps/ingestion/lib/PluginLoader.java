package com.datatorrent.apps.ingestion.lib;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.tools.ant.DirectoryScanner;

import com.datatorrent.stram.webapp.OperatorDiscoverer;
import com.datatorrent.stram.webapp.TypeGraph;

public class PluginLoader
{
  private static final String DT_PLUGINS_DIR = "/.dt/plugins";

  /**
   * Searches datatorrent plugins directory for plugin which implements superInterface and of given type and name
   *
   * @param superInterface fully qualified class/interface name which plugin should override
   * @param type type of the plugin e.g. compression
   * @param name name of plugin e.g. lzo
   * @return class of given type and name which implements superInterface from discovered plugins
   */
  public static String discoverPlugin(String superInterface, String type, String name)
  {
    List<String> paths = getJarsToScan();
    OperatorDiscoverer od = new OperatorDiscoverer(paths.toArray(new String[] {}));
    od.buildTypeGraph();
    TypeGraph typeGraph = od.getTypeGraph();
    Set<String> pluginClassNames = typeGraph.getDescendants(superInterface);
    for (String pluginClassName : pluginClassNames) {
      List<String> parents = typeGraph.getParents(pluginClassName);
      if (parents.contains(superInterface)) {
        return pluginClassName;
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
      if (libJar.endsWith(".jar ")) {
        result.add(dir + File.separator + libJar);
      }
    }
    return result;
  }
}
