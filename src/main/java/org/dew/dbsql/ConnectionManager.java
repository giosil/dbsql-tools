package org.dew.dbsql;

import java.sql.Connection;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.sql.DataSource;

public 
class ConnectionManager 
{
  public static String sJDBC_PATH = "jdbc/";
  
  public static final String sDEF_CONN_NAME = "";
  public static Boolean useSequece;
  
  public static final String sNOSQL_DBNAME = "";
  
  public static Connection getDefaultConnection() throws Exception {
    return getConnection(sDEF_CONN_NAME);
  }
  
  public static Connection getConnection(String sName) throws Exception {
    String sExceptions = "";
    Context ctx = new InitialContext();
    // Impostazione iniziale
    try {
      DataSource ds = (DataSource) ctx.lookup(sJDBC_PATH + sName);
      if (ds != null)
        return ds.getConnection();
    } catch (Exception ex) {
      sExceptions += "[" + ex.getMessage() + "]";
      ex.printStackTrace();
    }
    // Altri possibili percorsi...
    sJDBC_PATH = "java:/";
    try {
      DataSource ds = (DataSource) ctx.lookup(sJDBC_PATH + sName);
      if (ds != null)
        return ds.getConnection();
    } catch (Exception ex) {
      sExceptions += "[" + ex.getMessage() + "]";
    }
    sJDBC_PATH = "java:/jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(sJDBC_PATH + sName);
      if (ds != null)
        return ds.getConnection();
    } catch (Exception ex) {
      sExceptions += "[" + ex.getMessage() + "]";
    }
    sJDBC_PATH = "java:/comp/env/jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(sJDBC_PATH + sName);
      if (ds != null)
        return ds.getConnection();
    } catch (Exception ex) {
      sExceptions += "[" + ex.getMessage() + "]";
    }
    sJDBC_PATH = "jdbc/";
    try {
      DataSource ds = (DataSource) ctx.lookup(sJDBC_PATH + sName);
      if (ds != null)
        return ds.getConnection();
    } catch (Exception ex) {
      sExceptions += "[" + ex.getMessage() + "]";
    }
    throw new Exception("DataSource " + sName + " not available. (" + sExceptions + ")");
  }
  
  public static void closeConnection(Connection conn) {
    if(conn == null) return;
    try { conn.close(); } catch (Exception ex) {}
  }
  
  public static void close(AutoCloseable... arrayOfAutoCloseable) {
    if (arrayOfAutoCloseable == null || arrayOfAutoCloseable.length == 0) {
      return;
    }
    for (AutoCloseable autoCloseable : arrayOfAutoCloseable) {
      if (autoCloseable == null)
        continue;
      if (autoCloseable instanceof Connection) {
        closeConnection((Connection) autoCloseable);
        continue;
      }
      try {
        autoCloseable.close();
      } catch (Exception ignore) {
      }
    }
  }
}
