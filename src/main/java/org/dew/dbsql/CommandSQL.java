package org.dew.dbsql;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public 
class CommandSQL
{
  protected Connection conn;
  protected PrintStream ps;
  protected List<String> listCommands = new ArrayList<String>();
  
  protected final static int ORACLE   = 0;
  protected final static int MYSQL    = 1;
  protected final static int POSTGRES = 2;
  protected final static int HSQLDB   = 3;
  protected int iDatabase = ORACLE;
  
  protected String sDefSchema;
  
  public
  CommandSQL(Connection conn, String sSchema)
    throws Exception
  {
    this.conn   = conn;
    this.ps     = new PrintStream(new File(sSchema + "_cmd.log"));
    
    this.sDefSchema = sSchema;
    
    try {
      DatabaseMetaData dbmd = conn.getMetaData();
      String sDBProductName = dbmd.getDatabaseProductName();
      if(sDBProductName != null && sDBProductName.length() > 0) {
        sDBProductName = sDBProductName.trim().toLowerCase();
        this.iDatabase = ORACLE;
        if(sDBProductName.startsWith("m")) {
          this.iDatabase = MYSQL;
        }
        else if(sDBProductName.startsWith("p")) {
          this.iDatabase = POSTGRES;
        }
        else if(sDBProductName.startsWith("h")) {
          this.iDatabase = HSQLDB;
        }
      }
    }
    catch(Exception ex) {
    }
  }
  
  public static
  void main(String[] args)
  {
    if(args == null || args.length != 1) {
      System.err.println("Usage: CommandSQL data_source");
      System.exit(1);
    }
    System.out.println("CommandSQL ver. 1.0");
    System.out.println("-------------------");
    printHelp();
    Connection conn = null;
    try {
      conn = JdbcDataSource.getConnection(args[0]);
      if(conn == null) {
        System.err.println("Data source " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      
      CommandSQL tool = new CommandSQL(conn, JdbcDataSource.getUser(args[0]));
      tool.start();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try{ conn.close(); } catch(Exception ex) {};
    }
  }
  
  public static
  void printHelp()
  {
    System.out.println("help       = this guide");
    System.out.println("lc         = list commands");
    System.out.println("la         = list aliases");
    System.out.println("l [idx]    = last command [by index]");
    System.out.println("exit       = exit from command sql");
    System.out.println("bye        = exit from command sql");
    System.out.println("tables [s] = list tables");
    System.out.println("views  [s] = list views");
    System.out.println("cat        = list catalogs");
    System.out.println("schemas    = list schemas");
    System.out.println("ver        = print product version");
    System.out.println("desc <t>   = script table t");
    System.out.println("view <t>   = view 20 records of table t");
    System.out.println("exp  <t>   = export table t");
    System.out.println("exp  <q>   = export query result");
    System.out.println("blob <q>   = export blob field");
    System.out.println("auto <b>   = set auto commit");
  }
  
  public
  void start()
    throws Exception
  {
    Statement stm = null;
    ResultSet rs  = null;
    String sLast  = null;
    try {
      stm = conn.createStatement();
      do {
        String cmd = waitForInput();
        
        if(cmd == null) break;
        if(cmd.length() == 0) continue;
        if(cmd.endsWith(";")) {
          cmd = cmd.substring(0, cmd.length()-1);
        }
        if(cmd.equalsIgnoreCase("exit")) {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        if(cmd.equalsIgnoreCase("bye"))  {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        
        if(cmd.equalsIgnoreCase("help")) {
          printHelp();
          continue;
        }
        else if(cmd.equalsIgnoreCase("l")) {
          if(sLast != null && sLast.length() > 0) {
            cmd = sLast;
          }
          else {
            System.out.println("No commands executed");
            continue;
          }
        }
        else if(cmd.startsWith("L ") || cmd.startsWith("l ")) {
          String sIdx = cmd.substring(2).trim();
          int iIdx = 0;
          try { iIdx = Integer.parseInt(sIdx); } catch(Exception ex) {}
          if(listCommands != null && listCommands.size() > iIdx) {
            cmd = listCommands.get(iIdx);
          }
          else {
            System.out.println("Invalid index (" + iIdx + ")");
            continue;
          }
        }
        else if(cmd.equalsIgnoreCase("lc")) {
          if(listCommands != null && listCommands.size() > 0) {
            for(int i = 0; i < listCommands.size(); i++) {
              System.out.println(i + " " + listCommands.get(i));
            }
          }
          continue;
        }
        else if(cmd.equalsIgnoreCase("la")) {
          List<String> aliases = CommandAliases.getAliases();
          if(aliases != null && aliases.size() > 0) {
            for(int i = 0; i < aliases.size(); i++) {
              System.out.println(i + " " + aliases.get(i));
            }
          }
          continue;
        }
        
        if(cmd.startsWith("$") && cmd.length() > 1) {
          String sAlias = "";
          String sRight = "";
          int iSep = cmd.indexOf(' ');
          if(iSep > 0) {
            sAlias = cmd.substring(0,iSep);
            sRight = cmd.substring(iSep+1);
          }
          else {
            sAlias = cmd;
            sRight = "";
          }
          cmd = CommandAliases.getAlias(sAlias);
          if(cmd == null || cmd.length() == 0) {
            System.out.println("alias " + sAlias + " not found");
            continue;
          }
          if(sRight != null && sRight.length() > 0) {
            cmd += " " + sRight;
          }
        }
        
        ps.println("# " + cmd);
        if(!cmd.equals(sLast)) {
          listCommands.add(cmd);
        }
        sLast = cmd;
        
        if(cmd.startsWith("tables") || cmd.startsWith("TABLES") || cmd.startsWith("Tables")) {
          int iSep = cmd.indexOf(' ');
          if(iSep > 0) {
            printTables(cmd.substring(iSep + 1));
          }
          else {
            printTables(null);
          }
        }
        else if(cmd.startsWith("views") || cmd.startsWith("VIEWS") || cmd.startsWith("Views")) {
          int iSep = cmd.indexOf(' ');
          if(iSep > 0) {
            printViews(cmd.substring(iSep + 1));
          }
          else {
            printViews(null);
          }
        }
        else if(cmd.startsWith("cat") || cmd.startsWith("CAT") || cmd.startsWith("Cat")) {
          printCatalogs();
        }
        else if(cmd.startsWith("sche") || cmd.startsWith("SCHE") || cmd.startsWith("Sche")) {
          printSchemas();
        }
        else if(cmd.startsWith("ver") || cmd.startsWith("VER") || cmd.startsWith("Ver")) {
          printVersion();
        }
        else if(cmd.startsWith("auto") || cmd.startsWith("AUTO") || cmd.startsWith("Auto")) {
          String sFlag = "t";
          if(cmd.length() > 5) sFlag = cmd.substring(6);
          char c0 = sFlag != null && sFlag.length() > 0 ? sFlag.charAt(0) : 't';
          boolean b = c0 == 't' || c0 == 'T' || c0 == 'y' || c0 == 'Y' || c0 == 's' || c0 == 'S' || c0 == '1' || c0 == 'j';
          conn.setAutoCommit(b);
          System.out.println("setAutoCommit(" + b + ")");
          ps.println("setAutoCommit(" + b + ")");
        }
        else if(cmd.startsWith("desc ") || cmd.startsWith("DESC ") || cmd.startsWith("Desc ")) {
          String sTable = cmd.substring(5);
          if(sTable != null && sTable.length() > 0) {
            try {
              printCreateTable(sTable);
            }
            catch(Exception ex) {
              System.out.println(ex.getMessage());
              ps.println(ex.getMessage());
            }
          }
          else {
            System.out.println("Invalid table name");
            ps.println("Invalid table name");
          }
        }
        else if(cmd.startsWith("exp ") || cmd.startsWith("EXP ") || cmd.startsWith("Exp ")) {
          cmd = cmd.substring(4);
          boolean boSelect = cmd.startsWith("SELECT ") || cmd.startsWith("select ") || cmd.startsWith("Select ");
          if(!boSelect) {
            cmd = "SELECT * FROM " + cmd;
          }
          try {
            String sTable = "TABLE";
            int iFrom = cmd.indexOf("FROM");
            if(iFrom < 0) iFrom = cmd.indexOf("from");
            if(iFrom < 0) iFrom = cmd.indexOf("From");
            if(iFrom > 0) {
              int iEndTab = 0;
              int iSpace  = cmd.indexOf(' ', iFrom + 6);
              if(iSpace <= 0) {
                iEndTab = cmd.length();
              }
              else {
                iEndTab = iSpace;
              }
              if(iEndTab > 0) {
                sTable = cmd.substring(iFrom+4, iEndTab).trim();
                if(sTable.indexOf(',') >= 0 || sTable.indexOf(' ') >= 0) sTable = "TABLE";
              }
            }
            rs = stm.executeQuery(cmd);
            int iRows = exportResultSet(rs, sTable);
            rs.close();
            System.out.println(iRows + " rows exported.");
            ps.println(iRows + " rows exported.");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("view ") || cmd.startsWith("VIEW ") || cmd.startsWith("View ")) {
          cmd = cmd.substring(4);
          boolean boSelect = cmd.startsWith("SELECT ") || cmd.startsWith("select ") || cmd.startsWith("Select ");
          if(!boSelect) {
            cmd = "SELECT * FROM " + cmd;
          }
          try {
            rs = stm.executeQuery(cmd);
            printResultSet(rs, 0);
            rs.close();
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("blob ") || cmd.startsWith("BLOB ") || cmd.startsWith("Blob ")) {
          cmd = cmd.substring(5);
          boolean boSelect = cmd.startsWith("SELECT ") || cmd.startsWith("select ") || cmd.startsWith("Select ");
          if(!boSelect) {
            cmd = "SELECT * FROM " + cmd;
          }
          String sMessage = "";
          try {
            String sTable = "TABLE";
            int iFrom = cmd.indexOf("FROM");
            if(iFrom < 0) iFrom = cmd.indexOf("from");
            if(iFrom < 0) iFrom = cmd.indexOf("From");
            if(iFrom > 0) {
              int iEndTab = 0;
              int iSpace  = cmd.indexOf(' ', iFrom + 6);
              if(iSpace <= 0) {
                iEndTab = cmd.length();
              }
              else {
                iEndTab = iSpace;
              }
              if(iEndTab > 0) {
                sTable = cmd.substring(iFrom+4, iEndTab).trim();
                if(sTable.indexOf(',') >= 0 || sTable.indexOf(' ') >= 0) sTable = "TABLE";
              }
            }
            rs = stm.executeQuery(cmd);
            if(rs.next()) {
              byte[] abBlobContent = getBLOBContent(rs, 1);
              if(abBlobContent == null || abBlobContent.length == 0) {
                sMessage = "No blob found";
              }
              else {
                String sFileName = "blob_" + System.currentTimeMillis();
                String sFilePath = saveContent(abBlobContent, sFileName);
                sMessage = "Blob saved in " + sFilePath;
              }
            }
            else {
              sMessage = "No row found";
            }
            rs.close();
            System.out.println(sMessage);
            ps.println(sMessage);
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("select ") || cmd.startsWith("SELECT ") || cmd.startsWith("Select ")) {
          try {
            rs = stm.executeQuery(cmd);
            int iRows = printResultSet(rs, 20000);
            rs.close();
            System.out.println(iRows + " rows returned.");
            ps.println(iRows + " rows returned.");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("commit") || cmd.startsWith("COMMIT") || cmd.startsWith("Commit")) {
          try {
            conn.commit();
            System.out.println("commit executed.");
            ps.println("commit executed.");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("rollback") || cmd.startsWith("ROLLBACK") || cmd.startsWith("Rollback")) {
          try {
            conn.rollback();
            System.out.println("rollback executed.");
            ps.println("rollback executed.");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("update ") || cmd.startsWith("UPDATE ") || cmd.startsWith("Update ")) {
          try {
            int iRows = stm.executeUpdate(cmd);
            System.out.println(iRows + " updated");
            ps.println(iRows + " updated");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("delete ") || cmd.startsWith("DELETE ") || cmd.startsWith("Delete ")) {
          try {
            int iRows = stm.executeUpdate(cmd);
            System.out.println(iRows + " deleted");
            ps.println(iRows + " deleted");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else if(cmd.startsWith("insert ") || cmd.startsWith("INSERT ") || cmd.startsWith("Insert ")) {
          try {
            int iRows = stm.executeUpdate(cmd);
            System.out.println(iRows + " inserted");
            ps.println(iRows + " inserted");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else {
          try {
            int iRows = stm.executeUpdate(cmd);
            if(iRows > 0) {
              System.out.println(iRows + " affected");
              ps.println(iRows + " affected");
            }
            else {
              System.out.println("executed");
              ps.println("executed");
            }
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
      }
      while(true);
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(stm  != null) try{ stm.close();  } catch(Exception ex) {}
      try {
        if(conn != null && !conn.isClosed()) {
          conn.commit();
          conn.close();
        }
      }
      catch(Exception ex) {
      }
    }
  }
  
  protected 
  String waitForInput()
  {
    byte[] result = new byte[640];
    int length = 0;
    try {
      System.out.print("# ");
      length = System.in.read(result);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      return null;
    }
    if(length < 1) return "";
    return new String(result, 0, length).trim();
  }
  
  protected
  int printResultSet(ResultSet rs, int iMaxRows)
  {
    if(iMaxRows < 1) iMaxRows = 20;
    int iRows = 0;
    try {
      StringBuffer sbRow = new StringBuffer();
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      int[] columnTypes = new int[columnCount];
      for (int i = 0; i < columnCount; i++){
        sbRow.append(";" + rsmd.getColumnName(i + 1));
        columnTypes[i] = rsmd.getColumnType(i + 1);
      }
      if(sbRow.length() > 0) {
        System.out.println(sbRow.substring(1));
        ps.println(sbRow.substring(1));
      }
      while(rs.next()) {
        iRows++;
        sbRow.delete(0, sbRow.length());
        for (int i = 0; i < columnCount; i++){
          int t = columnTypes[i];
          String value = null;
          if(t == Types.BINARY || t == Types.BLOB || t == Types.CLOB) {
            value = "<BLOB>";
          }
          else {
            value = rs.getString(i + 1);
          }
          sbRow.append(";" + value);
        }
        if(sbRow.length() > 0) {
          System.out.println(sbRow.substring(1));
          ps.println(sbRow.substring(1));
        }
        if(iRows >= iMaxRows) break;
      }
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
    return iRows;
  }
  
  protected
  int exportResultSet(ResultSet rs, String sTable)
  {
    Calendar cal = Calendar.getInstance();
    int iRows = 0;
    String sHeader = "";
    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      int[] columnTypes = new int[columnCount];
      for (int i = 1; i <= columnCount; i++){
        sHeader += "," + rsmd.getColumnName(i);
        columnTypes[i - 1] = rsmd.getColumnType(i);
      }
      if(sHeader.length() == 0) return 0;
      if(sHeader.length() > 0) sHeader = sHeader.substring(1);
      while(rs.next()) {
        StringBuffer sbValues = new StringBuffer();
        for (int i = 1; i <= columnCount; i++){
          int t = columnTypes[i - 1];
          switch(t) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
              String sValue = rs.getString(i);
              if(sValue == null || sValue.length() == 0) {
                sbValues.append("''");
              }
              else {
                sbValues.append('\'');
                sbValues.append(sValue.replace("'", "''"));
                sbValues.append('\'');
              }
              break;
            case Types.DATE:
              java.sql.Date date = rs.getDate(i);
              if(date == null) {
                sbValues.append("NULL");
              }
              else {
                cal.setTimeInMillis(date.getTime());
                int iYear  = cal.get(Calendar.YEAR);
                int iMonth = cal.get(Calendar.MONTH) + 1;
                int iDay   = cal.get(Calendar.DAY_OF_MONTH);
                if(iYear < 1900) {
                  sbValues.append("NULL");
                }
                else {
                  String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
                  String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
                  sbValues.append("TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')");
                  // sbValues.append("'" + iYear + "-" + sMonth + "-" + sDay + "'");
                }
              }
              break;
            case Types.TIME:
            case Types.TIMESTAMP:
              java.sql.Timestamp timestamp = rs.getTimestamp(i);
              if(timestamp == null) {
                sbValues.append("NULL");
              }
              else {
                cal.setTimeInMillis(timestamp.getTime());
                int iYear   = cal.get(Calendar.YEAR);
                int iMonth  = cal.get(Calendar.MONTH) + 1;
                int iDay    = cal.get(Calendar.DAY_OF_MONTH);
                int iHour   = cal.get(Calendar.HOUR_OF_DAY);
                int iMinute = cal.get(Calendar.MINUTE);
                int iSecond  = cal.get(Calendar.SECOND);
                if(iYear < 1900) {
                  sbValues.append("NULL");
                }
                else {
                  String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
                  String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
                  sbValues.append("TO_TIMESTAMP('" + iYear + "-" + sMonth + "-" + sDay + " " + iHour + ":" + iMinute + ":" + iSecond + "','YYYY-MM-DD HH24:MI:SS')");
                  // sbValues.append("'" + iYear + "-" + sMonth + "-" + sDay + " " + iHour + ":" + iMinute + ":" + iSecond + "'");
                }
              }
              break;
            case Types.BINARY:
            case Types.BLOB:
            case Types.CLOB:
              byte[] abBlobContent = getBLOBContent(rs, i);
              if(abBlobContent == null || abBlobContent.length == 0) {
                sbValues.append("EMPTY_BLOB()");
              }
              else {
                String sBlobContent = new String(abBlobContent);
                if(sBlobContent.length() > 3000) sBlobContent = sBlobContent.substring(0, 3000);
                sbValues.append("utl_raw.cast_to_raw('" + sBlobContent.replace("'", "''") + "')");
              }
              break;
            default:
              sValue = rs.getString(i);
              if(sValue == null || sValue.length() == 0) {
                sbValues.append("NULL");
              }
              else {
                sbValues.append(sValue);
              }
          }
          if(i < columnCount) sbValues.append(',');
        }
        String sInsert = "INSERT INTO " + sTable + "(" + sHeader + ") VALUES(" + sbValues + ");";
        System.out.println(sInsert);
        ps.println(sInsert);
        iRows++;
        if(iRows >= 20000) break;
      }
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }    
    return iRows;
  }
  
  protected
  void printVersion()
  {
    try {
      DatabaseMetaData dbmd = conn.getMetaData();
      String sDBProductName = dbmd.getDatabaseProductName();
      String sDBProductVer  = dbmd.getDatabaseProductVersion();
      System.out.println(sDBProductName + " " + sDBProductVer);
      ps.println(sDBProductName + " " + sDBProductVer);
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printCatalogs()
  {
    try {
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = dbmd.getCatalogs();
      while (rs.next()){
        String catalogName = rs.getString(1);
        System.out.println(catalogName);
        ps.println(catalogName);
      }
      rs.close();
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printSchemas()
  {
    try {
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = dbmd.getSchemas();
      while (rs.next()){
        String schemaName = rs.getString(1);
        System.out.println(schemaName);
        ps.println(schemaName);
      }
      rs.close();
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printTables(String sSelSchema)
  {
    if(sSelSchema != null && sSelSchema.trim().length() == 0) {
      sSelSchema = null;
    }
    try {
      int iCount = 0;
      String[] types = new String[1];
      types[0] = "TABLE";
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = null;
      if(this.iDatabase == ORACLE) {
        if(sSelSchema != null && sSelSchema.length() > 0) {
          rs = dbmd.getTables(this.sDefSchema, sSelSchema, null, types);
        }
        else {
          rs = dbmd.getTables(this.sDefSchema, this.sDefSchema, null, types);
        }
      }
      else {
        rs = dbmd.getTables(null, sSelSchema, null, types);
      }
      while(rs.next()){
        String tableName = rs.getString(3);
        if(tableName.indexOf('$') >= 0 || tableName.equals("PLAN_TABLE")) continue;
        System.out.println(tableName);
        ps.println(tableName);
        iCount++;
      }
      rs.close();
      
      System.out.println(iCount + " tables returned");
      ps.println(iCount + " tables returned");
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printViews(String sSelSchema)
  {
    if(sSelSchema != null && sSelSchema.trim().length() == 0) {
      sSelSchema = null;
    }
    try {
      int iCount = 0;
      String[] types = new String[1];
      types[0] = "VIEW";
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = null;
      if(this.iDatabase == ORACLE) {
        if(sSelSchema != null && sSelSchema.length() > 0) {
          rs = dbmd.getTables(this.sDefSchema, sSelSchema, null, types);
        }
        else {
          rs = dbmd.getTables(this.sDefSchema, this.sDefSchema, null, types);
        }
      }
      else {
        rs = dbmd.getTables(null, sSelSchema, null, types);
      }
      while(rs.next()){
        String tableName = rs.getString(3);
        if(tableName.indexOf('$') >= 0 || tableName.equals("PLAN_TABLE")) continue;
        System.out.println(tableName);
        ps.println(tableName);
        iCount++;
      }
      rs.close();
      
      System.out.println(iCount + " views returned");
      ps.println(iCount + " views returned");
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      ps.println(ex.getMessage());
    }
  }
  
  protected
  void printCreateTable(String sTable)
    throws Exception
  {
    List<String> listPK = new ArrayList<String>();
    
    sTable = sTable.toUpperCase();
    
    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet rs = null;
    try{
      rs = dbmd.getPrimaryKeys(null, null, sTable);
      while (rs.next()) {
        listPK.add(rs.getString(4));
      }
      rs.close();
      
      String sPrimaryKeys = new String();
      for (int i=0; i<listPK.size(); i++) {
        String sPKName = (String) listPK.get(i);
        sPrimaryKeys += "," + sPKName;
      }
      if(sPrimaryKeys.length() > 0) {
        sPrimaryKeys = sPrimaryKeys.substring(1);
      }
      
      StringBuffer sb = new StringBuffer();
      rs = dbmd.getColumns(null, null, sTable, null);
      while (rs.next()) {
        String sFieldName = rs.getString(4);  // COLUMN_NAME
        int iFieldType    = rs.getInt(5);     // DATA_TYPE
        int iSize         = rs.getInt(7);     // COLUMN_SIZE
        int iDigits       = rs.getInt(9);     // DECIMAL_DIGITS
        int iNullable     = rs.getInt(11);    // NULLABLE
        String sDefValue  = rs.getString(13); // COLUMN_DEF
        String sNullable  = iNullable == DatabaseMetaData.columnNoNulls ? " NOT NULL" : "";
        String sDefault   = "";
        if(sDefValue != null && sDefValue.length() > 0) {
          if(iFieldType == java.sql.Types.VARCHAR || iFieldType == java.sql.Types.CHAR || iFieldType == java.sql.Types.BLOB || iFieldType == java.sql.Types.CLOB || 
              iFieldType == java.sql.Types.DATE || iFieldType == java.sql.Types.TIME || iFieldType == java.sql.Types.TIMESTAMP) {
            sDefault = " DEFAULT '" + sDefValue.replace("'", "''") + "'";
          }
          else {
            sDefault = " DEFAULT " + sDefValue;
          }
        }
        
        sFieldName = rpad(sFieldName, ' ', 18);
        
        if(iFieldType == java.sql.Types.VARCHAR) {
          if(iDatabase == ORACLE) {
            sb.append("\t" + sFieldName + " VARCHAR2(" + iSize + ")" + sDefault + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " VARCHAR(" + iSize + ")" + sDefault + sNullable + ",\n");
          }
        }
        else if(iFieldType == java.sql.Types.CHAR) {
          sb.append("\t" + sFieldName + " CHAR(" + iSize + ")" + sDefault + sNullable + ",\n");
        }
        else if(iFieldType == java.sql.Types.DATE) {
          sb.append("\t" + sFieldName + " DATE" + sDefault + sNullable + ",\n");
        }
        else if(iFieldType == java.sql.Types.TIME) {
          if(iDatabase == ORACLE) {
            sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sDefault + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " TIME" + sDefault + sNullable + ",\n");
          }
        }
        else if(iFieldType == java.sql.Types.TIMESTAMP) {
          if(iDatabase == ORACLE) {
            sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sDefault + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
        }
        else if(iFieldType == java.sql.Types.BLOB) {
          sb.append("\t" + sFieldName + " BLOB" + sDefault + sNullable + ",\n");
        }
        else if(iFieldType == java.sql.Types.CLOB) {
          if(iDatabase == ORACLE) {
            sb.append("\t" + sFieldName + " CBLOB" + sDefault + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
        }
        else if(iSize <= 20) {
          if(iDigits > 0) {
            if(iDatabase == ORACLE) {
              sb.append("\t" + sFieldName + " NUMBER(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " DECIMAL(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
          }
          else {
            if(iDatabase == ORACLE) {
              sb.append("\t" + sFieldName + " NUMBER(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
            }
          }
        }
        else {
          if(iDatabase == ORACLE) {
            sb.append("\t" + sFieldName + " NUMBER" + sDefault + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
          }
        }
      }
      if(sb.length() > 2) {
        System.out.println("CREATE TABLE " + sTable + "(");
        ps.println("CREATE TABLE " + sTable + "(");
        String sField = sb.substring(0, sb.length() - 2);
        if(sPrimaryKeys.length() > 0) {
          sField += ",\n\tCONSTRAINT PK_" + sTable + " PRIMARY KEY (" + sPrimaryKeys + ")";
        }
        System.out.print(sField);
        ps.println(sField);
        System.out.println(");");
        ps.println(");");
      }
      else {
        System.out.println("table " + sTable + " not found.");
        ps.println("table " + sTable + " not found.");
      }
    }
    finally {
      if(rs != null) try{ rs.close(); } catch(Exception ex) {}
    }
  }
  
  public static 
  String rpad(String text, char c, int length) 
  {
    if(text == null) text = "";
    int iTextLength = text.length();
    if(iTextLength >= length) return text;
    int diff = length - iTextLength;
    StringBuilder sb = new StringBuilder();
    sb.append(text);
    for(int i = 0; i < diff; i++) sb.append(c);
    return sb.toString();
  }
  
  public static
  byte[] getBLOBContent(ResultSet rs, int index)
    throws Exception
  {
    Blob blob = rs.getBlob(index);
    if(blob == null) return null;
    InputStream is = blob.getBinaryStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] abDataBuffer = new byte[1024];
    int iBytesRead = 0;
    while((iBytesRead = is.read(abDataBuffer, 0, abDataBuffer.length)) != -1) {
      baos.write(abDataBuffer, 0, iBytesRead);
    }
    baos.flush();
    return baos.toByteArray();
  }
  
  public static
  String saveContent(byte[] content, String sFilePath)
    throws Exception
  {
    if(content == null) return null;
    if(content == null || content.length == 0) return null;
    File file = null;
    FileOutputStream fos = null;
    try {
      file = new File(sFilePath);
      fos = new FileOutputStream(sFilePath);
      fos.write(content);
    }
    finally {
      if(fos != null) try{ fos.close(); } catch(Exception ex) {}
    }
    return file.getAbsolutePath();
  }
}
