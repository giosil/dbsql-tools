package org.dew.dbsql;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.List;

@SuppressWarnings({"rawtypes","unchecked"})
public 
class ExportData 
{
  protected Connection conn;
  
  protected String sORA_CATALOG;
  protected String sORA_SCHEMA;
  protected List<String> tables;
  
  public
  ExportData(Connection conn, String sSchema)
    throws Exception
  {
    this.conn = conn;
    
    this.sORA_CATALOG = sSchema;
    this.sORA_SCHEMA  = sSchema;
  }
  
  public
  ExportData(Connection conn, String sSchema, List<String> tables)
    throws Exception
  {
    this.conn = conn;
    
    this.sORA_CATALOG = sSchema;
    this.sORA_SCHEMA  = sSchema;
    this.tables       = tables;
  }
  
  public static
  void main(String[] args)
  {
    if(args == null || args.length != 1) {
      System.err.println("Usage: ExportData data_source");
      System.exit(1);
    }
    Connection conn = null;
    try {
      conn = JdbcDataSource.getConnection(args[0]);
      
      if(conn == null) {
        System.err.println("Data source " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      
      ExportData tool = new ExportData(conn, JdbcDataSource.getUser(args[0]));
      tool.export();
      
      System.out.println("Scripts generated in " + System.getProperty("user.home"));
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try{ conn.close(); } catch(Exception ex) {};
    }
  }
  
  public
  void export()
    throws Exception
  {
    PrintStream psD = getPrintStream(sORA_SCHEMA + "_dat.sql");
    PrintStream psS = getPrintStream(sORA_SCHEMA + "_seq.sql");
    int iDefMaxRows = 1000;
    
    psD.println("set define off;");
    psD.println("");
    List listTables = getTables();
    for(int i = 0; i < listTables.size(); i++) {
      String sTable = (String) listTables.get(i);
      if(tables != null && tables.size() > 0) {
        if(!tables.contains(sTable)) {
          continue;
        }
      }
      export(sTable, null, iDefMaxRows, psD, psS);
    }
    psD.close();
    psS.close();
    System.out.println("End.");
  }
  
  public
  List getTables()
    throws Exception
  {
    List listResult = new ArrayList();
    String[] types = new String[1];
    types[0] = "TABLE";
    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet rs = dbmd.getTables(sORA_CATALOG, sORA_SCHEMA, null, types);
    while (rs.next()){
      String tableName = rs.getString(3);
      if(tableName.equals("PLAN_TABLE")) continue;
      listResult.add(tableName);
    }
    rs.close();
    return listResult;
  }
  
  public
  void export(String sTable, String sWhere, int iMaxRows, PrintStream psD, PrintStream psS)
    throws Exception
  {
    System.out.println("Export " + sTable);
    
    Calendar cal = Calendar.getInstance();
    
    String sSQL = "SELECT * FROM " + sTable;
    if(sWhere != null && sWhere.length() > 2) {
      if(sWhere.startsWith("WHERE ") || sWhere.startsWith("where")) {
        sSQL += " " + sWhere; 
      }
      else {
        sSQL += " WHERE " + sWhere;
      }
    }
    
    Statement stm = null;
    ResultSet rs = null;
    try {
      String sPK = getPK(sTable);
      if(sPK != null && sPK.length() > 0) {
        sSQL += " ORDER BY " + sPK;
      }
      else {
        sSQL += " ORDER BY 1";
      }
      
      stm = conn.createStatement();
      rs  = stm.executeQuery(sSQL);
      
      int iRows = 0;
      int iFirstValue    = 0;
      int iMaxFirstValue = 0;
      String sHeader = "";
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      int[] columnTypes = new int[columnCount];
      for (int i = 1; i <= columnCount; i++){
        sHeader += "," + rsmd.getColumnName(i);
        columnTypes[i - 1] = rsmd.getColumnType(i);
      }
      if(sHeader.length() == 0) return;
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
              int iSecond = cal.get(Calendar.SECOND);
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
              if(i == 1 && psD != null) {
                try { iFirstValue = Integer.parseInt(sValue); } catch(Exception ex) {}
                if(iFirstValue > iMaxFirstValue) iMaxFirstValue = iFirstValue;
              }
              sbValues.append(sValue);
            }
          }
          if(i < columnCount) sbValues.append(',');
        }
        String sInsert = "INSERT INTO " + sTable + "(" + sHeader + ") VALUES(" + sbValues + ");";
        psD.println(sInsert);
        iRows++;
        if(iMaxRows > 0 && iRows >= iMaxRows) break;
      }
      psD.println("COMMIT;");
      if(iMaxFirstValue > 0 && psS != null) {
        int iStartWith = iMaxFirstValue + 1;
        psS.println("DROP SEQUENCE SEQ_" + sTable + ";");
        psS.println("CREATE SEQUENCE SEQ_" + sTable + " START WITH " + iStartWith + " MAXVALUE 999999999999 MINVALUE 1 NOCYCLE NOCACHE NOORDER;");
        psS.println("");
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  protected
  String getPK(String sTable)
    throws Exception
  {
    String sResult = "";
    ResultSet rs = null;
    try {
      DatabaseMetaData dbmd = conn.getMetaData();
      rs = dbmd.getPrimaryKeys(null, null, sTable);
      while (rs.next()) {
        sResult += "," + rs.getString(4);
      }
      if(sResult.length() > 0) sResult = sResult.substring(1);
    }
    finally {
      if(rs != null) try{ rs.close(); } catch(Exception ex) {}
    }
    return sResult;
  }
  
  protected static
  PrintStream getPrintStream(String sFileName)
  {
    if(sFileName != null && sFileName.length() > 0) {
      String sUserHome = System.getProperty("user.home");
      try {
        FileOutputStream oFOS = new FileOutputStream(sUserHome + File.separator + sFileName, false);
        return new PrintStream(oFOS, true);
      }
      catch (FileNotFoundException ex) {
        ex.printStackTrace();
        return System.out;
      }
    }
    return System.out;
  }  
  
  protected static
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
}