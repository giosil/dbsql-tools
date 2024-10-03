package org.dew.dbsql;

import java.io.InputStream;

import java.net.URL;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.Properties;

public
class JdbcDataSource
{
  public static Properties config = new Properties();
  
  static {
    InputStream is = null;
    try {
      URL urlCfg = Thread.currentThread().getContextClassLoader().getResource("jdbc.cfg");
      if(urlCfg != null) {
        is = urlCfg.openStream();
        config.load(is);
      }
      else {
        System.out.println("GRAVE: jdbc.cfg not found in classpath.");
      }
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try { is.close(); } catch (Exception ex) {}
    }
  }
  
  public static
  String getSchema(String sName)
    throws Exception
  {
    String url = config.getProperty(sName + ".url");
    if(url == null || url.length() == 0) {
      return config.getProperty(sName + ".user");
    }
    else if(url.indexOf("mariadb:") >= 0 || url.indexOf("mysql:") >= 0) {
      int sep = url.lastIndexOf('/');
      if(sep > 0 && sep < url.length() -1) {
        return url.substring(sep + 1);
      }
      else {
        return config.getProperty(sName + ".user");
      }
    }
    return config.getProperty(sName + ".user");
  }
  
  public static
  String getUser(String sName)
    throws Exception
  {
    return config.getProperty(sName + ".user");
  }
  
  public static
  Connection getConnection(String sName)
    throws Exception
  {
    String sDriver = config.getProperty(sName + ".driver");
    if(sDriver == null || sDriver.length() == 0) return null;
    Class.forName(sDriver);
    
    String sURL  = config.getProperty(sName + ".url");
    if(sURL == null || sURL.length() == 0) return null;
    String sUser = config.getProperty(sName + ".user");
    String sPass = config.getProperty(sName + ".password");
    
    System.out.println("Create connection at " + sURL + " with " + sUser + " (autocommit=false)...");
    
    Connection conn = DriverManager.getConnection(sURL, sUser, sPass);
    conn.setAutoCommit(false);
    return conn;
  }
}
