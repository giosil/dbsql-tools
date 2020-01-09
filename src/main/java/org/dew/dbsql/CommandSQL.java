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

@SuppressWarnings({"rawtypes","unchecked"})
public 
class CommandSQL
{
  protected Connection conn;
  protected String schema;
  protected PrintStream ps;
  protected List<String> listCommands = new ArrayList<String>();
  
  public
  CommandSQL(Connection conn, String sSchema)
    throws Exception
  {
    this.conn   = conn;
    this.schema = sSchema;
    this.ps     = new PrintStream(new File(sSchema + "_cmd.log"));
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
    System.out.println("help     = this guide");
    System.out.println("lc       = list commands");
    System.out.println("la       = list aliases");
    System.out.println("l [idx]  = last command [by index]");
    System.out.println("exit     = exit from command sql");
    System.out.println("bye      = exit from command sql");
    System.out.println("tables   = list tables");
    System.out.println("cat      = list catalogs");
    System.out.println("schemas  = list schemas");
    System.out.println("ver      = print product version");
    System.out.println("desc <t> = script table t");
    System.out.println("view <t> = view 20 records of table t");
    System.out.println("exp  <t> = export table t");
    System.out.println("exp  <q> = export query result");
    System.out.println("blob <q> = export blob field");
    System.out.println("auto <b> = set auto commit");
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
        String sSQL = waitForInput();
        
        if(sSQL == null) break;
        if(sSQL.length() == 0) continue;
        if(sSQL.endsWith(";")) {
          sSQL = sSQL.substring(0, sSQL.length()-1);
        }
        if(sSQL.equalsIgnoreCase("exit")) {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        if(sSQL.equalsIgnoreCase("bye"))  {
          System.out.println("bye");
          ps.println("bye at " + new Date());
          break;
        }
        
        if(sSQL.equalsIgnoreCase("help")) {
          printHelp();
          continue;
        }
        else
        if(sSQL.equalsIgnoreCase("l")) {
          if(sLast != null && sLast.length() > 0) {
            sSQL = sLast;
          }
          else {
            System.out.println("No commands executed");
            continue;
          }
        }
        else
        if(sSQL.startsWith("L ") || sSQL.startsWith("l ")) {
          String sIdx = sSQL.substring(2).trim();
          int iIdx = 0;
          try { iIdx = Integer.parseInt(sIdx); } catch(Exception ex) {}
          if(listCommands != null && listCommands.size() > iIdx) {
            sSQL = listCommands.get(iIdx);
          }
          else {
            System.out.println("Invalid index (" + iIdx + ")");
            continue;
          }
        }
        else
        if(sSQL.equalsIgnoreCase("lc")) {
          if(listCommands != null && listCommands.size() > 0) {
            for(int i = 0; i < listCommands.size(); i++) {
              System.out.println(i + " " + listCommands.get(i));
            }
          }
          continue;
        }
        else
        if(sSQL.equalsIgnoreCase("la")) {
          List aliases = CommandAliases.getAliases();
          if(aliases != null && aliases.size() > 0) {
            for(int i = 0; i < aliases.size(); i++) {
              System.out.println(i + " " + aliases.get(i));
            }
          }
          continue;
        }
        
        if(sSQL.startsWith("$") && sSQL.length() > 1) {
          String sAlias = "";
          String sRight = "";
          int iSep = sSQL.indexOf(' ');
          if(iSep > 0) {
            sAlias = sSQL.substring(0,iSep);
            sRight = sSQL.substring(iSep+1);
          }
          else {
            sAlias = sSQL;
            sRight = "";
          }
          sSQL = CommandAliases.getAlias(sAlias);
          if(sSQL == null || sSQL.length() == 0) {
            System.out.println("alias " + sAlias + " not found");
            continue;
          }
          if(sRight != null && sRight.length() > 0) {
            sSQL += " " + sRight;
          }
        }
        
        ps.println("# " + sSQL);
        if(!sSQL.equals(sLast)) {
          listCommands.add(sSQL);
        }
        sLast = sSQL;
        
        if(sSQL.startsWith("tables") || sSQL.startsWith("TABLES") || sSQL.startsWith("Tables")) {
          printTables();
        }
        else
        if(sSQL.startsWith("cat") || sSQL.startsWith("CAT") || sSQL.startsWith("Cat")) {
          printCatalogs();
        }
        else
        if(sSQL.startsWith("sche") || sSQL.startsWith("SCHE") || sSQL.startsWith("Sche")) {
          printSchemas();
        }
        else
        if(sSQL.startsWith("ver") || sSQL.startsWith("VER") || sSQL.startsWith("Ver")) {
          printVersion();
        }
        else
        if(sSQL.startsWith("auto") || sSQL.startsWith("AUTO") || sSQL.startsWith("Auto")) {
          String sFlag = "t";
          if(sSQL.length() > 5) sFlag = sSQL.substring(6);
          char c0 = sFlag != null && sFlag.length() > 0 ? sFlag.charAt(0) : 't';
          boolean b = c0 == 't' || c0 == 'T' || c0 == 'y' || c0 == 'Y' || c0 == 's' || c0 == 'S' || c0 == '1' || c0 == 'j';
          conn.setAutoCommit(b);
          System.out.println("setAutoCommit(" + b + ")");
          ps.println("setAutoCommit(" + b + ")");
        }
        else
        if(sSQL.startsWith("desc ") || sSQL.startsWith("DESC ") || sSQL.startsWith("Desc ")) {
          String sTable = sSQL.substring(5);
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
        else
        if(sSQL.startsWith("exp ") || sSQL.startsWith("EXP ") || sSQL.startsWith("Exp ")) {
          sSQL = sSQL.substring(4);
          boolean boSelect = sSQL.startsWith("SELECT ") || sSQL.startsWith("select ") || sSQL.startsWith("Select ");
          if(!boSelect) {
            sSQL = "SELECT * FROM " + sSQL;
          }
          try {
            String sTable = "TABLE";
            int iFrom = sSQL.indexOf("FROM");
            if(iFrom < 0) iFrom = sSQL.indexOf("from");
            if(iFrom < 0) iFrom = sSQL.indexOf("From");
            if(iFrom > 0) {
              int iEndTab = 0;
              int iSpace  = sSQL.indexOf(' ', iFrom + 6);
              if(iSpace <= 0) {
                iEndTab = sSQL.length();
              }
              else {
                iEndTab = iSpace;
              }
              if(iEndTab > 0) {
                sTable = sSQL.substring(iFrom+4, iEndTab).trim();
                if(sTable.indexOf(',') >= 0 || sTable.indexOf(' ') >= 0) sTable = "TABLE";
              }
            }
            rs = stm.executeQuery(sSQL);
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
        else
        if(sSQL.startsWith("view ") || sSQL.startsWith("VIEW ") || sSQL.startsWith("View ")) {
          sSQL = sSQL.substring(4);
          boolean boSelect = sSQL.startsWith("SELECT ") || sSQL.startsWith("select ") || sSQL.startsWith("Select ");
          if(!boSelect) {
            sSQL = "SELECT * FROM " + sSQL;
          }
          try {
            rs = stm.executeQuery(sSQL);
            printResultSet(rs, 0);
            rs.close();
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else
        if(sSQL.startsWith("blob ") || sSQL.startsWith("BLOB ") || sSQL.startsWith("Blob ")) {
          sSQL = sSQL.substring(5);
          boolean boSelect = sSQL.startsWith("SELECT ") || sSQL.startsWith("select ") || sSQL.startsWith("Select ");
          if(!boSelect) {
            sSQL = "SELECT * FROM " + sSQL;
          }
          String sMessage = "";
          try {
            String sTable = "TABLE";
            int iFrom = sSQL.indexOf("FROM");
            if(iFrom < 0) iFrom = sSQL.indexOf("from");
            if(iFrom < 0) iFrom = sSQL.indexOf("From");
            if(iFrom > 0) {
              int iEndTab = 0;
              int iSpace  = sSQL.indexOf(' ', iFrom + 6);
              if(iSpace <= 0) {
                iEndTab = sSQL.length();
              }
              else {
                iEndTab = iSpace;
              }
              if(iEndTab > 0) {
                sTable = sSQL.substring(iFrom+4, iEndTab).trim();
                if(sTable.indexOf(',') >= 0 || sTable.indexOf(' ') >= 0) sTable = "TABLE";
              }
            }
            rs = stm.executeQuery(sSQL);
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
        else
        if(sSQL.startsWith("select ") || sSQL.startsWith("SELECT ") || sSQL.startsWith("Select ")) {
          try {
            rs = stm.executeQuery(sSQL);
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
        else
        if(sSQL.startsWith("commit") || sSQL.startsWith("COMMIT") || sSQL.startsWith("Commit")) {
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
        else
        if(sSQL.startsWith("rollback") || sSQL.startsWith("ROLLBACK") || sSQL.startsWith("Rollback")) {
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
        else 
        if(sSQL.startsWith("update ") || sSQL.startsWith("UPDATE ") || sSQL.startsWith("Update ")) {
          try {
            int iRows = stm.executeUpdate(sSQL);
            System.out.println(iRows + " updated");
            ps.println(iRows + " updated");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else 
        if(sSQL.startsWith("delete ") || sSQL.startsWith("DELETE ") || sSQL.startsWith("Delete ")) {
          try {
            int iRows = stm.executeUpdate(sSQL);
            System.out.println(iRows + " deleted");
            ps.println(iRows + " deleted");
          }
          catch(Exception ex) {
            System.out.println(ex.getMessage());
            ps.println(ex.getMessage());
          }
        }
        else 
        if(sSQL.startsWith("insert ") || sSQL.startsWith("INSERT ") || sSQL.startsWith("Insert ")) {
          try {
            int iRows = stm.executeUpdate(sSQL);
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
            int iRows = stm.executeUpdate(sSQL);
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
      if(conn != null) try{
        conn.commit();
        conn.close(); 
      } 
      catch(Exception ex) {
        ex.printStackTrace();
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
  void printTables()
  {
    try {
      int iCount = 0;
      String[] types = new String[1];
      types[0] = "TABLE";
      DatabaseMetaData dbmd = conn.getMetaData();
      ResultSet rs = dbmd.getTables(schema, schema, null, types);
      while(rs.next()){
        String tableName = rs.getString(3);
        if(tableName.equals("PLAN_TABLE")) continue;
        System.out.println(tableName);
        ps.println(tableName);
        iCount++;
      }
      rs.close();
      
      if(iCount == 0) {
        ResultSet rsS = dbmd.getSchemas();
        while(rsS.next()) {
          String schemaName = rsS.getString(1);
          
          ResultSet rsT = dbmd.getTables(schemaName, schemaName, null, types);
          while (rsT.next()){
            String tableName = rsT.getString(3);
            if(tableName.equals("PLAN_TABLE")) continue;
            System.out.println(schemaName + "." + tableName);
            ps.println(schemaName + "." + tableName);
            iCount++;
          }
          rsT.close();
          
        }
        rsS.close();
      }
      
      System.out.println(iCount + " tables returned");
      ps.println(iCount + " tables returned");
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
    List listPK = new ArrayList();
    
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
        String sFieldName = rs.getString(4);
        int iFieldType    = rs.getInt(5);
        int iSize         = rs.getInt(7);
        int iDigits       = rs.getInt(9);
        int iNullable     = rs.getInt(11);
        String sNullable  = iNullable == DatabaseMetaData.columnNoNulls ? " NOT NULL" : "";
        
        sFieldName = rpad(sFieldName, ' ', 18);
        
        if(iFieldType == java.sql.Types.VARCHAR) {
          sb.append("\t" + sFieldName + " VARCHAR2(" + iSize + ")" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.CHAR) {
          sb.append("\t" + sFieldName + " CHAR(" + iSize + ")" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.DATE) {
          sb.append("\t" + sFieldName + " DATE" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.TIME) {
          sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.TIMESTAMP) {
          sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.BLOB) {
          sb.append("\t" + sFieldName + " BLOB" + sNullable + ",\n");
        }
        else
        if(iFieldType == java.sql.Types.CLOB) {
          sb.append("\t" + sFieldName + " CBLOB" + sNullable + ",\n");
        }
        else {
          if(iSize <= 20) {
            sb.append("\t" + sFieldName + " NUMBER(" + iSize + "," + iDigits + ")" + sNullable + ",\n");
          }
          else {
            sb.append("\t" + sFieldName + " NUMBER" + sNullable + ",\n");
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
    StringBuffer sb = new StringBuffer();
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
