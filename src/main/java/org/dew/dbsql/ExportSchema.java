package org.dew.dbsql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public
class ExportSchema
{
  protected Connection conn;
  
  protected PrintStream out     = null;
  protected PrintStream out_fk  = null;
  protected PrintStream out_idx = null;
  
  protected String sDefSchema;
  
  protected final static int ORACLE   = 0;
  protected final static int MYSQL    = 1;
  protected final static int POSTGRES = 2;
  protected final static int HSQLDB   = 3;
  
  protected String sDestination;
  protected int iDestination = ORACLE;
  
  protected String sFILE;
  protected String sFILE_FK;
  protected String sFILE_IDX;
  
  public
  ExportSchema(Connection conn, String sSchema, String sDestination)
    throws Exception
  {
    this.conn = conn;
    
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
    
    this.sFILE        = System.getProperty("user.home") + File.separator + sDefSchema + ".sql";
    this.sFILE_FK     = System.getProperty("user.home") + File.separator + sDefSchema + "_FK.sql";
    this.sFILE_IDX    = System.getProperty("user.home") + File.separator + sDefSchema + "_IDX.sql";
    
    this.out     = getPrintStream(sFILE);
    this.out_fk  = getPrintStream(sFILE_FK);
    this.out_idx = getPrintStream(sFILE_IDX);
  }
  
  public static
  void main(String[] args)
  {
    if(args == null || args.length == 0) {
      System.err.println("Usage: ExportSchema data_source [oracle|mysql|mariadb|postgres|hsqldb|h2]");
      System.exit(1);
    }
    Connection conn = null;
    try {
      conn = JdbcDataSource.getConnection(args[0]);
      
      if(conn == null) {
        System.err.println("Data source " + args[0] + " not exists in configuration file.");
        System.exit(1);
      }
      
      String sDestination = args.length > 1 ? args[1] : "oracle";
      
      ExportSchema tool = new ExportSchema(conn, JdbcDataSource.getSchema(args[0]), sDestination);
      tool.writeScripts();
      
      System.out.println("Scripts generated in " + System.getProperty("user.home") + ".");
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      try{ conn.close(); } catch(Exception ex) {};
    }
  }
  
  public
  void writeScripts()
    throws Exception
  {
    printHeader();
    
    System.out.println("Read catalog " + sDefSchema + "...");
    // Lettura tabelle
    List<String> listTables = getTables();
    
    for(int i = 0; i < listTables.size(); i++) {
      String sTable = listTables.get(i);
      
      System.out.println(sTable);
      
      // Scrittura comandi CREATE TABLE
      printCreateTable(sTable, false);
      
      // Scrittura comandi ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
      printForeignKeys(sTable);
      
      // Scrittura comandi CREATE INDEX
      printIndexes(sTable);
    }
    
    printFooter();
    
    if(iDestination == HSQLDB && out != null) {
      out.println("SHUTDOWN COMPACT;\n");
    }
    if(iDestination == HSQLDB && out_fk != null) {
      out_fk.println("\n");
      out_fk.println("SHUTDOWN COMPACT;\n");
    }
    if(iDestination == HSQLDB && out_idx != null) {
      out_idx.println("\n");
      out_idx.println("SHUTDOWN COMPACT;\n");
    }
  }
  
  public
  void printHeader()
    throws Exception
  {
    out.println("--");
    out.println("-- Tables of schema " + sDefSchema);
    out.println("--");
    out.println();
    
    out_fk.println("--");
    out_fk.println("-- Foreign Keys Alter tables of schema " + sDefSchema);
    out_fk.println("--");
    out_fk.println();
    
    out_idx.println("--");
    out_idx.println("-- Indexes of schema " + sDefSchema);
    out_idx.println("--");
    out_idx.println();
  }
  
  public
  List<String> getTables()
    throws Exception
  {
    List<String> listResult = new ArrayList<String>();
    
    String[] types = new String[1];
    types[0] = "TABLE";
    DatabaseMetaData dbmd = conn.getMetaData();
    
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
  void printCreateTable(String sTable, boolean boDrop)
    throws Exception
  {
    List<String> listPK = new ArrayList<String>();
    
    if(boDrop) out.println("DROP TABLE " + sTable);
    
    out.println("CREATE TABLE " + sTable + "(");
    
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
            if(sDefValue.startsWith("'") && sDefValue.endsWith("'") && sDefValue.length() > 1) {
              sDefault = " DEFAULT " + sDefValue;
            }
            else if(sDefValue.equalsIgnoreCase("NULL")) {
              sDefault = " DEFAULT NULL";
            }
            else {
              sDefault = " DEFAULT '" + sDefValue.replace("'", "''").replace("\\", "\\\\") + "'";
            }
          }
          else {
            sDefault = " DEFAULT " + sDefValue;
          }
        }
        
        sFieldName = rpad(sFieldName, ' ', 18);
        
        switch (iDestination) {
        case MYSQL:
          if(iFieldType == java.sql.Types.VARCHAR) {
            if(iSize > 255) {
              sb.append("\t" + sFieldName + " TEXT " + sDefault + sNullable + ",\n");
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
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.TIMESTAMP) {
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.BLOB) {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.CLOB) {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.LONGVARCHAR) {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
          else if(iSize <= 20) {
            if(iDigits > 0) {
              sb.append("\t" + sFieldName + " DECIMAL(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
            }
          }
          else {
            sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
          }
          
          break;
          
        case POSTGRES:
          if(iFieldType == java.sql.Types.VARCHAR) {
            if(iSize > 255) {
              sb.append("\t" + sFieldName + " VARCHAR(" + iSize + ")" + sDefault + sNullable + ",\n");
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
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.TIMESTAMP) {
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.BLOB) {
            sb.append("\t" + sFieldName + " BYTEA" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.CLOB) {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.LONGVARCHAR) {
            sb.append("\t" + sFieldName + " TEXT" + sDefault + sNullable + ",\n");
          }
          else if(iSize <= 20) {
            if(iDigits > 0) {
              sb.append("\t" + sFieldName + " NUMERIC(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " INTEGER" + sDefault + sNullable + ",\n");
            }
          }
          else {
            sb.append("\t" + sFieldName + " INTEGER" + sDefault + sNullable + ",\n");
          }
          
          break;
          
        case HSQLDB:
          if(iFieldType == java.sql.Types.VARCHAR) {
            if(iSize > 255) {
              sb.append("\t" + sFieldName + " VARCHAR(" + iSize + ")" + sDefault + sNullable + ",\n");
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
            sb.append("\t" + sFieldName + " TIME" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.TIMESTAMP) {
            sb.append("\t" + sFieldName + " TIMESTAMP" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.BLOB) {
            sb.append("\t" + sFieldName + " BLOB" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.CLOB) {
            sb.append("\t" + sFieldName + " VARCHAR(16777216)" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.LONGVARCHAR) {
            sb.append("\t" + sFieldName + " VARCHAR(16777216)" + sDefault + sNullable + ",\n");
          }
          else if(iSize <= 20) {
            if(iDigits > 0) {
              sb.append("\t" + sFieldName + " DECIMAL(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else if(iSize > 10) {
              sb.append("\t" + sFieldName + " BIGINT" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
            }
          }
          else {
            sb.append("\t" + sFieldName + " INT" + sDefault + sNullable + ",\n");
          }
          
          break;
        
        default:
          if(iFieldType == java.sql.Types.VARCHAR) {
            if(iSize > 255) {
              sb.append("\t" + sFieldName + " VARCHAR2(" + iSize + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " VARCHAR2(" + iSize + ")" + sDefault + sNullable + ",\n");
            }
          }
          else if(iFieldType == java.sql.Types.CHAR) {
            sb.append("\t" + sFieldName + " CHAR(" + iSize + ")" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.DATE) {
            sb.append("\t" + sFieldName + " DATE" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.TIME) {
            sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.TIMESTAMP) {
            sb.append("\t" + sFieldName + " TIMESTAMP(6)" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.BLOB) {
            sb.append("\t" + sFieldName + " BLOB" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.CLOB) {
            sb.append("\t" + sFieldName + " CBLOB" + sDefault + sNullable + ",\n");
          }
          else if(iFieldType == java.sql.Types.LONGVARCHAR) {
            sb.append("\t" + sFieldName + " CBLOB" + sDefault + sNullable + ",\n");
          }
          else if(iSize <= 20) {
            if(iDigits > 0) {
              sb.append("\t" + sFieldName + " NUMBER(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
            else {
              sb.append("\t" + sFieldName + " NUMBER(" + iSize + "," + iDigits + ")" + sDefault + sNullable + ",\n");
            }
          }
          else {
            sb.append("\t" + sFieldName + " NUMBER" + sDefault + sNullable + ",\n");
          }
          
          break;
        }
        
      }
      String sField = sb.substring(0, sb.length() - 2);
      if(sPrimaryKeys.length() > 0) {
        sField += ",\n\tCONSTRAINT PK_" + sTable + " PRIMARY KEY (" + sPrimaryKeys + ")";
      }
      out.print(sField);
      out.println(");" + "\n");
    }
    finally {
      if(rs != null) try{ rs.close(); } catch(Exception ex) {};
    }
  }
  
  public
  void printForeignKeys(String sTable)
    throws Exception
  {
    List<String> listForeignKeys = new ArrayList<String>();
    
    DatabaseMetaData dbmd = conn.getMetaData();
    
    Map<String, String[]> mapForeingKeys = new HashMap<String, String[]>();
    ResultSet rs = dbmd.getImportedKeys(null, null, sTable);
    while (rs.next()) {
      String sForeignTable = rs.getString(3);
      String sForeignField = rs.getString(4);
      String sTableField   = rs.getString(8);
      String sFKName       = rs.getString(12);
      String[] asFields = (String[]) mapForeingKeys.get(sFKName);
      if(asFields == null) {
        asFields = new String[3];
        asFields[0] = sForeignTable;
        asFields[1] = sTableField;
        asFields[2] = sForeignField;
        mapForeingKeys.put(sFKName, asFields);
      }
      else {
        asFields[1] += "," + sTableField;
        asFields[2] += "," + sForeignField;
      }
      if(!listForeignKeys.contains(sFKName)) {
        listForeignKeys.add(sFKName);
      }
    }
    rs.close();
    
    Collections.sort(listForeignKeys);
    
    for(int i = 0; i < listForeignKeys.size(); i++) {
      String sFKName = listForeignKeys.get(i);
      
      String[] ssFields = mapForeingKeys.get(sFKName);
      String sForeignTable  = ssFields[0];
      String sTableFields   = ssFields[1];
      String sForeignFields = ssFields[2];
      
      String sAlter = "ALTER TABLE " + sTable + " ";
      if(iDestination == HSQLDB) {
        sAlter += "ADD FOREIGN KEY";
      }
      else {
        sAlter += "ADD CONSTRAINT FOREIGN KEY";
      }
      sAlter += " (" + sTableFields + ") REFERENCES ";
      sAlter += sForeignTable + " (" + sForeignFields + ");";
      
      out_fk.println(sAlter);
    }
  }
  
  public
  void printIndexes(String sTable)
    throws Exception
  {
    List<String> listIndexes = new ArrayList<String>();
    
    DatabaseMetaData dbmd = conn.getMetaData();
    
    Map<String, String> mapIndexes = new HashMap<String, String>();
    ResultSet rs = dbmd.getIndexInfo(null, null, sTable, false, true);
    
    while (rs.next()) {
      int iNonUnique    = rs.getInt(4);
      String sIndexName = rs.getString(6);
      String sFieldName = rs.getString(9);
      
      if(iNonUnique == 0)    continue;
      if(sIndexName == null) continue;
      
      String sFields = mapIndexes.get(sIndexName);
      if(sFields == null) {
        mapIndexes.put(sIndexName, sFieldName);
      }
      else {
        sFields += ", " + sFieldName;
        mapIndexes.put(sIndexName, sFields);
      }
      
      if(!listIndexes.contains(sIndexName)) {
        listIndexes.add(sIndexName);
      }
    }
    rs.close();
    
    Collections.sort(listIndexes);
    
    for(int i = 0; i < listIndexes.size(); i++) {
      String sIndexName = (String) listIndexes.get(i);
      String sFields = (String) mapIndexes.get(sIndexName);
      sIndexName = rpad(sIndexName, ' ', 18);
      String sCreate = "CREATE INDEX " + sIndexName + " ON " + sTable + "(" + sFields + ");";
      out_idx.println(sCreate);
    }
  }
  
  public
  void printFooter()
    throws Exception
  {
    out.println("\n");
  }
  
  protected static
  PrintStream getPrintStream(String sFileName)
  {
    if(sFileName != null){
      try{
        FileOutputStream fileoutputstream = new FileOutputStream(sFileName, false);
        return new PrintStream(fileoutputstream, true);
      }
      catch(FileNotFoundException ex){
        ex.printStackTrace();
      }
      return System.out;
    }
    else{
      return System.out;
    }
  }
  
  protected static
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
}