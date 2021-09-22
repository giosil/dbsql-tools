package org.dew.dbsql;

import java.io.InputStream;

import java.net.URL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public
class CommandAliases
{
  public static Properties aliases = new Properties();
  
  static {
    InputStream is = null;
    try {
      URL urlCfg = Thread.currentThread().getContextClassLoader().getResource("alias.cfg");
      if(urlCfg != null) {
        is = urlCfg.openStream();
        aliases.load(is);
      }
      else {
        System.out.println("WARNING: alias.cfg not found in classpath.");
      }
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try { is.close(); } catch (Exception oEx) {}
    }
  }
  
  public static
  String getAlias(String alias)
    throws Exception
  {
    if(aliases == null || alias == null) return null;
    
    String result = aliases.getProperty(alias.trim());
    if(result == null) {
      if(alias.startsWith("$") && alias.length() > 0) {
        result = aliases.getProperty(alias.substring(1).trim());
      }
    }
    return result;
  }
  
  public static
  List<String> getAliases()
    throws Exception
  {
    if(aliases == null) return new ArrayList<String>(0);
    
    List<String> listResult = new ArrayList<String>();
    Iterator<Object> iterator = aliases.keySet().iterator();
    while(iterator.hasNext()) {
      listResult.add((String) iterator.next());
    }
    Collections.sort(listResult);
    for(int i = 0; i < listResult.size(); i++) {
      String key = (String) listResult.get(i);
      String val = aliases.getProperty(key);
      if(val != null && val.length() > 70) {
        val = val.substring(0, 70) + "...";
      }
      listResult.set(i, key + " = " + val);
    }
    return listResult;
  }
}