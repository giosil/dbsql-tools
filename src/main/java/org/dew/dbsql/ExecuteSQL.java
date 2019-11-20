package org.dew.dbsql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public
class ExecuteSQL
{
  public static 
  void main(String[] args)
  {
    if(args == null || args.length != 2) {
      System.err.println("Usage: ExecuteSQL data_source sql_file");
      System.exit(1);
    }
    File file = new File(args[1]);
    if(!file.exists()) {
      System.err.println("File " + file.getAbsolutePath() + " not exists.");
      System.exit(1);
    }
    if(!file.isFile()) {
      System.err.println("File " + file.getAbsolutePath() + " is not a file.");
      System.exit(1);
    }
    int iErrors       = 0;
    int iLastLineErr  = 0;
    Connection conn   = null;
    Statement stm     = null;
    ResultSet rs      = null;
    BufferedReader br = null;
    String sLine = null;
    int iLine    = 0;
    StringBuffer sbSQL = new StringBuffer();
    String sNextSQLStatement = "";
    try {
      conn = JdbcDataSource.getConnection(args[0]);
      if(conn == null) {
        System.err.println("Data source " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      stm = conn.createStatement();
      br  = new BufferedReader(new FileReader(file));
      while((sLine = br.readLine()) != null) {
        iLine++;
        sLine = sLine.trim();
        if(sLine.length() == 0)    continue;
        if(sLine.startsWith("--")) continue;
        if(sLine.startsWith("//")) continue;
        if(sLine.startsWith("/*")) continue;
        if(sLine.startsWith("*/")) continue;
        if(sLine.equals("/")) sLine = ";";
        
        int iSep = sLine.lastIndexOf(';');
        if(iSep < 0) {
          sbSQL.append(" " + sLine);
          sNextSQLStatement = "";
          continue;
        }
        else {
          sbSQL.append(" " + sLine.substring(0, iSep));
          sNextSQLStatement = sLine.substring(iSep+1);
        }
        
        String sSQL = sbSQL.toString().trim();
        if(sSQL.startsWith("SELECT ") || sSQL.startsWith("select ") || sSQL.startsWith("Select ")) {
          try {
            System.out.println(sSQL);
            System.out.println("------------------------------------------------------------------------------------------------------------------------");
            rs = stm.executeQuery(sSQL);
            int iRows = printResultSet(rs, 20000);
            rs.close();
            System.out.println("------------------------------------------------------------------------------------------------------------------------");
            System.out.println("[" + iRows + " rows returned]");
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        else
        if(sSQL.startsWith("COMMIT") || sSQL.startsWith("commit") || sSQL.startsWith("Commit")) {
          try {
            System.out.println("[commit]");
            conn.commit();
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        else
        if(sSQL.startsWith("SET ") || sSQL.startsWith("set ") || sSQL.startsWith("Set ")) {
          // Ignored
          System.out.println("[ignored] "  + sSQL);
        }
        else
        if(sSQL.startsWith("@") || sSQL.startsWith("#") || sSQL.startsWith("$")) {
          // Ignored
          System.out.println("[ignored] "  + sSQL);
        }
        else
        if(sSQL.startsWith("SPOOL") || sSQL.startsWith("spool") || sSQL.startsWith("Spool")) {
          // Ignored
          System.out.println("[ignored] "  + sSQL);
        }
        else 
        if(sSQL.startsWith("UPDATE ") || sSQL.startsWith("update ") || sSQL.startsWith("Update ")) {
          try {
            System.out.println(sSQL);
            int iRows = stm.executeUpdate(sSQL);
            System.out.println("[" + iRows + " updated]");
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        else 
        if(sSQL.startsWith("DELETE ") || sSQL.startsWith("delete ") || sSQL.startsWith("Delete ")) {
          try {
            System.out.println(sSQL);
            int iRows = stm.executeUpdate(sSQL);
            System.out.println("[" + iRows + " deleted]");
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        else 
        if(sSQL.startsWith("INSERT ") || sSQL.startsWith("insert ") || sSQL.startsWith("Insert ")) {
          try {
            System.out.println(sSQL);
            int iRows = stm.executeUpdate(sSQL);
            System.out.println("[" + iRows + " inserted]");
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        else {
          try {
            System.out.println(sSQL);
            int iRows = stm.executeUpdate(sSQL);
            if(iRows > 0) {
              System.out.println("[" + iRows + " affected]");
            }
            else {
              System.out.println("[executed]");
            }
          }
          catch(Exception ex) {
            iErrors++;
            iLastLineErr = iLine;
            printMessage(ex, iLine);
          }
        }
        
        sbSQL.delete(0, sbSQL.length());
        sbSQL.append(sNextSQLStatement);
      }
    }
    catch(Exception ex) {
      iErrors++;
      iLastLineErr = iLine;
      printMessage(ex, iLine);
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(stm  != null) try{ stm.close();  } catch(Exception ex) {}
      if(conn != null) try{
        conn.commit();
        conn.close(); 
      } 
      catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("----------------------------------------");
    System.out.println("Errors: " + iErrors + ", Last Line Err.: " + iLastLineErr);
    System.out.println("----------------------------------------");
    System.out.println("End.");
  }
  
  protected static
  int printResultSet(ResultSet rs, int iMaxRows)
    throws Exception
  {
    if(iMaxRows < 1) iMaxRows = 20;
    int iRows = 0;
    StringBuffer sbRow = new StringBuffer();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    for (int i = 0; i < columnCount; i++){
      sbRow.append(";" + rsmd.getColumnName(i + 1));
    }
    if(sbRow.length() > 0) {
      System.out.println(sbRow.substring(1));
    }
    while(rs.next()) {
      iRows++;
      sbRow.delete(0, sbRow.length());
      for (int i = 0; i < columnCount; i++){
        String value = rs.getString(i + 1);
        sbRow.append(";" + value);
      }
      if(sbRow.length() > 0) {
        System.out.println(sbRow.substring(1));
      }
      if(iRows >= iMaxRows) break;
    }
    return iRows;
  }
  
  protected static
  void printMessage(Throwable th, int iLine) 
  {
    String sMessage = th.getMessage();
    if(sMessage == null) sMessage = th.toString();
    if(sMessage == null) sMessage = "Exception";
    sMessage = sMessage.replace('\n', ' ');
    System.out.println("[" + sMessage + "]{" + iLine + "}");
  }
}
