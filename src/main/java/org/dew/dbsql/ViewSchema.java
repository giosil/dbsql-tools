package org.dew.dbsql;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"rawtypes","unchecked"})
public 
class ViewSchema 
{
  protected Connection conn;
  protected int iDEF_MAX_ROWS;
  protected String sDefSchema;
  
  public
  ViewSchema(Connection conn, String sSchema, int iDefMaxRows)
    throws Exception
  {
    this.conn          = conn;
    this.iDEF_MAX_ROWS = iDefMaxRows;
    this.sDefSchema    = sSchema;
    if(iDEF_MAX_ROWS < 1) iDEF_MAX_ROWS = 20;
  }
  
  public static
  void main(String[] args)
  {
    if(args == null || args.length == 0) {
      System.err.println("Usage: ViewSchema data_source [max_rows]");
      System.exit(1);
    }
    int iDefMaxRows = 0;
    if(args.length > 1) {
      try{ iDefMaxRows = Integer.parseInt(args[1]); } catch(Exception ex) {}
    }
    Connection conn = null;
    try {
      conn = JdbcDataSource.getConnection(args[0]);
      
      ViewSchema tool = new ViewSchema(conn, JdbcDataSource.getUser(args[0]), iDefMaxRows);
      tool.view();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try{ conn.close(); } catch(Exception ex) {};
    }
  }
  
  public
  void view()
    throws Exception
  {
    List listTables = getTables();
    for(int i = 0; i < listTables.size(); i++) {
      String sTable = (String) listTables.get(i);
      view(sTable, null, iDEF_MAX_ROWS);
    }
  }
  
  public
  List getTables()
    throws Exception
  {
    List listResult = new ArrayList();
    
    String[] types = new String[1];
    types[0] = "TABLE";
    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet rs = dbmd.getTables(null, null, null, types);
    while(rs.next()){
      String schema    = rs.getString(2);
      String tableName = rs.getString(3);
      
      if(schema != null && schema.startsWith("APEX_")) continue;
      if(schema != null && schema.startsWith("SYS"))   continue;
      if(schema != null && schema.endsWith("SYS"))     continue;
      if(tableName.indexOf('$') >= 0 || tableName.equals("PLAN_TABLE")) continue;
      
      listResult.add(tableName);
    }
    rs.close();
    
    return listResult;
  }
  
  public
  void view(String sTable, String sWhere, int iMaxRows)
      throws Exception
  {
    System.out.println(sTable);
    String sSQL = "SELECT * FROM " + sTable;
    if(sWhere != null && sWhere.length() > 2) {
      if(sWhere.startsWith("WHERE ") || sWhere.startsWith("where")) {
        sSQL += " " + sWhere; 
      }
      else {
        sSQL += " WHERE " + sWhere;
      }
    }
    
    int iRows = 0;
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
