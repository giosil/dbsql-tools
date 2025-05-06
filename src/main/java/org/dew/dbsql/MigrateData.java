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

public 
class MigrateData 
{
  protected Connection connSrc;
  protected Connection connDst;
  
  protected String sDefSchema;
  
  protected final static int ORACLE   = 0;
  protected final static int MYSQL    = 1;
  protected final static int POSTGRES = 2;
  protected final static int HSQLDB   = 3;
  
  protected String sDestination;
  protected int iDestination = ORACLE;
  
  protected List<String> tables;
  
  protected int maxRows = 0;
  protected boolean lowercase = false;
  
  protected static String TAB_SEQUENCES = "TAB_SEQUENCES";
  
  public
  MigrateData(Connection connSrc, Connection connDst, String sSchema, String sDestination)
    throws Exception
  {
    this.connSrc = connSrc;
    this.connDst = connDst;
    
    this.sDefSchema = sSchema;
    
    this.sDestination = sDestination;
    if(this.sDestination == null || this.sDestination.length() == 0) {
      this.sDestination = "oracle";
    }
    else {
      this.sDestination = this.sDestination.trim().toLowerCase();
    }
    this.iDestination = ORACLE;
    if(this.sDestination.startsWith("m")) {
      this.iDestination = MYSQL;
    }
    else if(this.sDestination.startsWith("p")) {
      this.iDestination = POSTGRES;
    }
    else if(this.sDestination.startsWith("h")) {
      this.iDestination = HSQLDB;
    }
    if(this.sDestination.endsWith("lc")) {
      lowercase = true;
    }
  }
  
  public
  MigrateData(Connection connSrc, Connection connDst, String sSchema, String sDestination, List<String> tables)
    throws Exception
  {
    this.connSrc = connSrc;
    this.connDst = connDst;
    
    this.sDefSchema = sSchema;
    
    this.sDestination = sDestination;
    if(this.sDestination == null || this.sDestination.length() == 0) {
      this.sDestination = "oracle";
    }
    else {
      this.sDestination = this.sDestination.trim().toLowerCase();
    }
    this.iDestination = ORACLE;
    if(this.sDestination.startsWith("m")) {
      this.iDestination = MYSQL;
    }
    else if(this.sDestination.startsWith("p")) {
      this.iDestination = POSTGRES;
    }
    else if(this.sDestination.startsWith("h")) {
      this.iDestination = HSQLDB;
    }
    if(this.sDestination.endsWith("lc")) {
      lowercase = true;
    }
    
    this.tables = tables;
  }
  
  public int getMaxRows() {
    return maxRows;
  }

  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }
  
  public static
  void main(String[] args)
  {
    if(args == null || args.length < 2) {
      System.err.println("Usage: ExportData data_source_src data_source_dst [oracle|mysql|mariadb|postgres|hsqldb|h2][_lc] [maxRows] [tables]");
      System.exit(1);
    }
    Connection connSrc = null;
    Connection connDst = null;
    try {
      connSrc = JdbcDataSource.getConnection(args[0]);
      
      if(connSrc == null) {
        System.err.println("Data source src " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      
      connDst = JdbcDataSource.getConnection(args[1]);
      
      if(connDst == null) {
        System.err.println("Data source dest " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      
      String sDestination = args.length > 2 ? args[2] : "oracle";
      
      int iMaxRows = 0;
      String sMaxRows = args.length > 3 ? args[3] : "0";
      if(sMaxRows == null || sMaxRows.length() == 0) {
        sMaxRows = "0";
      }
      try {
        iMaxRows = Integer.parseInt(sMaxRows);
      }
      catch(Exception ex) {
      }
      
      String tables = args.length > 4 ? args[4] : null;
      
      MigrateData tool = new MigrateData(connSrc, connDst, JdbcDataSource.getSchema(args[0]), sDestination, stringToListUC(tables));
      tool.setMaxRows(iMaxRows);
      tool.export();
      
      System.out.println("Scripts generated in " + System.getProperty("user.home"));
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      if(connSrc != null) try{ connSrc.close(); } catch(Exception ex) {}
      if(connDst != null) try{ connDst.close(); } catch(Exception ex) {}
    }
  }
  
  public
  void export()
    throws Exception
  {
    PrintStream psD = getPrintStream(sDefSchema + "_dat.sql");
    PrintStream psS = getPrintStream(sDefSchema + "_seq.sql");
    
    if(iDestination == ORACLE) {
      psD.println("set define off;");
      psD.println("");
    }
    
    if(TAB_SEQUENCES == null || TAB_SEQUENCES.length() == 0) {
      TAB_SEQUENCES = "TAB_SEQUENCES";
    }
    if(lowercase) {
      TAB_SEQUENCES = TAB_SEQUENCES.toLowerCase();
    }
    
    if(iDestination == MYSQL) {
      psS.println("CREATE TABLE " + TAB_SEQUENCES + "(SEQ_NAME VARCHAR(50) NOT NULL,SEQ_VAL INT NOT NULL,CONSTRAINT PK_TAB_SEQUENCES PRIMARY KEY (SEQ_NAME));\n");
    }
    else if(iDestination == POSTGRES) {
      psS.println("CREATE TABLE " + TAB_SEQUENCES + "(SEQ_NAME VARCHAR(50) NOT NULL,SEQ_VAL INTEGER NOT NULL,CONSTRAINT PK_TAB_SEQUENCES PRIMARY KEY (SEQ_NAME));\n");
    }
    else if(iDestination == HSQLDB) {
      psS.println("CREATE TABLE " + TAB_SEQUENCES + "(SEQ_NAME VARCHAR(50) NOT NULL,SEQ_VAL INT NOT NULL,CONSTRAINT PK_TAB_SEQUENCES PRIMARY KEY (SEQ_NAME));\n");
    }
    
    List<String> listTables = getTables();
    for(int i = 0; i < listTables.size(); i++) {
      String sTable = (String) listTables.get(i);
      if(tables != null && tables.size() > 0) {
        if(!tables.contains(sTable.toUpperCase())) {
          continue;
        }
        if(tables.contains("-" + sTable.toUpperCase())) {
          continue;
        }
      }
      export(sTable, null, maxRows, psD, psS);
    }
    
    if(iDestination == HSQLDB) {
      psD.println("\n");
      psD.println("SHUTDOWN COMPACT;\n");
      
      psS.println("\n");
      psS.println("SHUTDOWN COMPACT;\n");
    }
    
    psD.close();
    psS.close();
    
    System.out.println("End.");
  }
  
  public
  List<String> getTables()
    throws Exception
  {
    List<String> listResult = new ArrayList<String>();
    
    String[] types = new String[1];
    types[0] = "TABLE";
    DatabaseMetaData dbmd = connSrc.getMetaData();
    
    ResultSet rs = null;
    String sDBProductName   = dbmd.getDatabaseProductName();
    String sDBProductNameLC = sDBProductName != null ? sDBProductName.trim().toLowerCase() : "";
    if(sDBProductNameLC.startsWith("o") || sDBProductNameLC.startsWith("m")) {
      rs = dbmd.getTables(sDefSchema, sDefSchema, null, types);
    }
    else {
      rs = dbmd.getTables(null, null, null, types);
    }
    while(rs.next()){
      String tableName = rs.getString(3);
      if(tableName.indexOf('$') >= 0 || tableName.equals("PLAN_TABLE")) continue;
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
    Statement stmDst = null;
    
    Statement stm = null;
    ResultSet rs = null;
    try {
      stmDst = connDst.createStatement();
      
      String sPK = getPK(sTable);
      if(sPK != null && sPK.length() > 0) {
        sSQL += " ORDER BY " + sPK;
      }
      else {
        sSQL += " ORDER BY 1";
      }
      
      stm = connSrc.createStatement();
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
                switch (iDestination) {
                case ORACLE:
                  sbValues.append("TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')");
                  break;
                
                default:
                  sbValues.append("'" + iYear + "-" + sMonth + "-" + sDay + "'");
                  break;
                }
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
                String sMonth  = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
                String sDay    = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
                
                String sHour   = iHour   < 10 ? "0" + iHour   : String.valueOf(iHour);
                String sMinute = iMinute < 10 ? "0" + iMinute : String.valueOf(iMinute);
                String sSecond = iSecond < 10 ? "0" + iSecond : String.valueOf(iSecond);
                
                switch (iDestination) {
                case ORACLE:
                  sbValues.append("TO_TIMESTAMP('" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "','YYYY-MM-DD HH24:MI:SS')");
                  break;
                default:
                  sbValues.append("'" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "'");
                  break;
                }
              }
            }
            break;
          case Types.BINARY:
          case Types.BLOB:
          case Types.CLOB:
            byte[] abBlobContent = getBLOBContent(rs, i);
            if(abBlobContent == null || abBlobContent.length == 0) {
              switch (iDestination) {
              case ORACLE:
                sbValues.append("EMPTY_BLOB()");
                break;
              
              default:
                sbValues.append("NULL");
                break;
              }
            }
            else {
              String sBlobContent = new String(abBlobContent);
              switch (iDestination) {
              case ORACLE:
                if(sBlobContent.length() > 3000) sBlobContent = sBlobContent.substring(0, 3000);
                sbValues.append("utl_raw.cast_to_raw('" + sBlobContent.replace("'", "''").replace("\n", "\\n") + "')");
                break;
              
              default:
                sbValues.append("'" + sBlobContent.replace("'", "''").replace("\n", "\\n") + "'");
                break;
              }
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
        String sTableName = lowercase ? sTable.toLowerCase() : sTable;
        String sInsert = "INSERT INTO " + sTableName + "(" + sHeader + ") VALUES(" + sbValues + ")";
        psD.println(sInsert + ";");
        iRows++;
        
        try {
          stmDst.execute(sInsert);
          connDst.commit();
          if(iRows % 100 == 0) {
            System.out.println("    " + iRows);
          }
        }
        catch(Exception ex) {
          System.err.println("[" + iRows + "] " + sInsert + ": " + ex);
        }
        if(iMaxRows > 0 && iRows >= iMaxRows) break;
      }
      
      if(iRows > 0) {
        psD.println("COMMIT;");
      }
      
      if(iDestination == ORACLE) {
        if(iMaxFirstValue > 0 && psS != null) {
          int iStartWith = iMaxFirstValue + 1;
          psS.println("DROP SEQUENCE SEQ_" + sTable + ";");
          psS.println("CREATE SEQUENCE SEQ_" + sTable + " START WITH " + iStartWith + " MAXVALUE 999999999999 MINVALUE 1 NOCYCLE NOCACHE NOORDER;");
          psS.println("");
        }
      }
      else {
        if(iMaxFirstValue > 0 && psS != null) {
          int iStartWith = iMaxFirstValue + 1;
          psS.println("INSERT INTO " + TAB_SEQUENCES + " VALUES('SEQ_" + sTable + "', " + iStartWith + ");");
          psS.println("COMMIT;");
        }
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
      if(stmDst != null) try{ stmDst.close(); } catch(Exception ex) {}
    }
  }
  
  protected
  String getPK(String sTable)
    throws Exception
  {
    String sResult = "";
    ResultSet rs = null;
    try {
      DatabaseMetaData dbmd = connSrc.getMetaData();
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
  
  protected static
  List<String> stringToListUC(String sText)
  {
    if(sText == null || sText.length() == 0) return new ArrayList<String>(0);
    if(sText.startsWith("[") && sText.endsWith("]")) {
      sText = sText.substring(1, sText.length()-1);
    }
    ArrayList<String> arrayList = new ArrayList<String>();
    int iIndexOf = 0;
    int iBegin   = 0;
    iIndexOf     = sText.indexOf(',');
    while(iIndexOf >= 0) {
      arrayList.add(sText.substring(iBegin, iIndexOf).trim().toUpperCase());
      iBegin = iIndexOf + 1;
      iIndexOf = sText.indexOf(',', iBegin);
    }
    arrayList.add(sText.substring(iBegin).trim().toUpperCase());
    return arrayList;
  }
}

