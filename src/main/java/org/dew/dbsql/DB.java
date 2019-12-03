package org.dew.dbsql;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public
class DB
{
  public static final String sFIELDS = "#f";
  
  public static
  int readInt(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    int iResult = 0;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) iResult = rs.getInt(1);
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return iResult;
  }
  
  public static
  int readInt(Connection conn, int iDefault, String sSQL, Object... parameters)
    throws Exception
  {
    int iResult = 0;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) {
        iResult = rs.getInt(1);
      }
      else {
        iResult = iDefault;
      }
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return iResult;
  }
  
  public static
  double readDouble(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    double dResult = 0;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) dResult = rs.getDouble(1);
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return dResult;
  }
  
  public static
  double readDouble(Connection conn, double dDefault, String sSQL, Object... parameters)
    throws Exception
  {
    double dResult = 0;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) {
        dResult = rs.getDouble(1);
      }
      else {
        dResult = dDefault;
      }
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return dResult;
  }
  
  public static
  Calendar readCalendar(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    Calendar calResult = null;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) {
        Date date = rs.getDate(1);
        if(date != null) {
          calResult = Calendar.getInstance();
          calResult.setTimeInMillis(date.getTime());
        }
      }
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return calResult;
  }
  
  public static
  Calendar readDateTime(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    Calendar calResult = null;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) {
        Timestamp timestamp = rs.getTimestamp(1);
        if(timestamp != null) {
          calResult = Calendar.getInstance();
          calResult.setTimeInMillis(timestamp.getTime());
        }
      }
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return calResult;
  }
  
  public static
  String readString(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    String result = null;
    PreparedStatement pstm = null;
    ResultSet rs  = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      rs = pstm.executeQuery();
      if(rs.next()) result = rs.getString(1);
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return result;
  }
  
  public static
  int execUpd(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    int result = 0;
    PreparedStatement pstm = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      result = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
    return result;
  }
  
  public static
  Map<String, Object> read(Connection conn, String sSQL)
    throws Exception
  {
    Statement stm = null;
    try {
      stm = conn.createStatement();
      return toMap(stm.executeQuery(sSQL));
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  Map<String, Object> read(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      return toMap(pstm.executeQuery());
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Map<String, Object>> readList(Connection conn, String sSQL)
    throws Exception
  {
    Statement stm = null;
    try {
      stm = conn.createStatement();
      return toList(stm.executeQuery(sSQL));
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Map<String, Object>> readList(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      return toList(pstm.executeQuery());
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Integer> readListOfInteger(Connection conn, String sSQL)
    throws Exception
  {
    Statement stm = null;
    try {
      stm = conn.createStatement();
      return toListOfInteger(stm.executeQuery(sSQL));
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Integer> readListOfInteger(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      return toListOfInteger(pstm.executeQuery());
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Double> readListOfDouble(Connection conn, String sSQL)
    throws Exception
  {
    Statement stm = null;
    try {
      stm = conn.createStatement();
      return toListOfDouble(stm.executeQuery(sSQL));
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<Double> readListOfDouble(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      return toListOfDouble(pstm.executeQuery());
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<String> readListOfString(Connection conn, String sSQL)
    throws Exception
  {
    Statement stm = null;
    try {
      stm = conn.createStatement();
      return toListOfString(stm.executeQuery(sSQL));
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  List<String> readListOfString(Connection conn, String sSQL, Object... parameters)
    throws Exception
  {
    PreparedStatement pstm = null;
    ResultSet rs = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      setParameters(pstm, 0, parameters);
      return toListOfString(pstm.executeQuery());
    }
    finally {
      if(rs   != null) try{ rs.close();   } catch(Exception ex) {}
      if(pstm != null) try{ pstm.close(); } catch(Exception ex) {}
    }
  }
  
  public static
  Map<String, Object> toMap(ResultSet rs)
    throws Exception
  {
    Map<String,Object> mapResult = null;
    
    List<String> listFields = new ArrayList<String>();
    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      int iColumnCount = rsmd.getColumnCount();
      if(rs.next()) {
        mapResult = new HashMap<String,Object>();
        mapResult.put(sFIELDS, listFields);
        for(int i = 1; i <= iColumnCount; i++) {
          String sField  = rsmd.getColumnName(i);
          int iFieldType = rsmd.getColumnType(i);
          if(iFieldType == java.sql.Types.CHAR || iFieldType == java.sql.Types.VARCHAR) {
            mapResult.put(sField, rs.getString(i));
          }
          else
          if(iFieldType == java.sql.Types.DATE) {
            mapResult.put(sField, rs.getDate(i));
          }
          else
          if(iFieldType == java.sql.Types.TIME || iFieldType == java.sql.Types.TIMESTAMP) {
            mapResult.put(sField, rs.getTimestamp(i));
          }
          else
          if(iFieldType == java.sql.Types.BINARY || iFieldType == java.sql.Types.BLOB || iFieldType == java.sql.Types.CLOB) {
            mapResult.put(sField, getBLOBContent(rs, i));
          }
          else {
            String sValue = rs.getString(i);
            if(sValue != null) {
              if(sValue.indexOf('.') >= 0 || sValue.indexOf(',') >= 0) {
                mapResult.put(sField, new Double(rs.getDouble(i)));
              }
              else {
                mapResult.put(sField, new Integer(rs.getInt(i)));
              }
            }
            else {
              mapResult.put(sField, null);
            }
          }
          listFields.add(sField);
        }
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
    }
    return mapResult;
  }
  
  public static
  List<Map<String, Object>> toList(ResultSet rs)
    throws Exception
  {
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>>();
    
    List<String> listFields = new ArrayList<String>();
    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      int iColumnCount = rsmd.getColumnCount();
      while(rs.next()) {
        Map<String,Object> mapResult = new HashMap<String,Object>();
        if(listResult.size() == 0) {
          mapResult.put(sFIELDS, listFields);
        }
        listResult.add(mapResult);
        
        for(int i = 1; i <= iColumnCount; i++) {
          String sField  = rsmd.getColumnName(i);
          int iFieldType = rsmd.getColumnType(i);
          if(iFieldType == java.sql.Types.CHAR || iFieldType == java.sql.Types.VARCHAR) {
            mapResult.put(sField, rs.getString(i));
          }
          else
          if(iFieldType == java.sql.Types.DATE) {
            mapResult.put(sField, rs.getDate(i));
          }
          else
          if(iFieldType == java.sql.Types.TIME || iFieldType == java.sql.Types.TIMESTAMP) {
            mapResult.put(sField, rs.getTimestamp(i));
          }
          else
          if(iFieldType == java.sql.Types.BINARY || iFieldType == java.sql.Types.BLOB || iFieldType == java.sql.Types.CLOB) {
            mapResult.put(sField, getBLOBContent(rs, i));
          }
          else {
            String sValue = rs.getString(i);
            if(sValue != null) {
              if(sValue.indexOf('.') >= 0 || sValue.indexOf(',') >= 0) {
                mapResult.put(sField, new Double(rs.getDouble(i)));
              }
              else {
                mapResult.put(sField, new Integer(rs.getInt(i)));
              }
            }
            else {
              mapResult.put(sField, null);
            }
          }
          listFields.add(sField);
        }
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
    }
    return listResult;
  }
  
  public static
  List<Integer> toListOfInteger(ResultSet rs)
    throws Exception
  {
    List<Integer> listResult = new ArrayList<Integer>();
    try {
      while(rs.next()) {
        listResult.add(rs.getInt(1));
      }
    }
    finally {
      if(rs != null) try{ rs.close();  } catch(Exception ex) {}
    }
    return listResult;
  }
  
  public static
  List<Double> toListOfDouble(ResultSet rs)
    throws Exception
  {
    List<Double> listResult = new ArrayList<Double>();
    try {
      while(rs.next()) {
        listResult.add(rs.getDouble(1));
      }
    }
    finally {
      if(rs != null) try{ rs.close();  } catch(Exception ex) {}
    }
    return listResult;
  }
  
  public static
  List<String> toListOfString(ResultSet rs)
    throws Exception
  {
    List<String> listResult = new ArrayList<String>();
    try {
      while(rs.next()) {
        listResult.add(rs.getString(1));
      }
    }
    finally {
      if(rs != null) try{ rs.close();  } catch(Exception ex) {}
    }
    return listResult;
  }
  
  public static
  boolean insert(Connection conn, String sTable, Map<String,Object> mapValues)
    throws Exception
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return false;
    }
    
    List<String> listFields = getFields(mapValues);
    if(listFields == null || listFields.size() == 0) {
      return false;
    }
    
    String sSQL = "INSERT INTO " + sTable;
    String sFields = "";
    for(int i = 0; i < listFields.size(); i++) {
      sFields += "," + listFields.get(i);
    }
    sSQL += "(" + sFields.substring(1) + ") VALUES ";
    String sValues = "";
    for(int i = 0; i < listFields.size(); i++) sValues += ",?";
    sSQL += "(" + sValues.substring(1) + ")";
    
    int iResult = 0;
    PreparedStatement pstm = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      for(int i = 0; i < listFields.size(); i++) {
        String sField = listFields.get(i);
        Object parameter = mapValues.get(sField);
        if(parameter instanceof Integer) {
          pstm.setInt(i + 1, ((Integer) parameter).intValue());
        }
        else if(parameter instanceof Long) {
          pstm.setLong(i + 1, ((Long) parameter).longValue());
        }
        else if(parameter instanceof Double) {
          pstm.setDouble(i + 1, ((Double) parameter).doubleValue());
        }
        else if(parameter instanceof String) {
          pstm.setString(i + 1, (String) parameter);
        }
        else if(parameter instanceof Boolean) {
          pstm.setInt(i + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
        }
        else if(parameter instanceof java.util.Calendar) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
        }
        else if(parameter instanceof java.sql.Date) {
          pstm.setDate(i + 1, (java.sql.Date) parameter);
        }
        else if(parameter instanceof java.sql.Timestamp) {
          pstm.setTimestamp(i + 1, (java.sql.Timestamp) parameter);
        }
        else if(parameter instanceof java.sql.Time) {
          pstm.setTime(i + 1, (java.sql.Time) parameter);
        }
        else if(parameter instanceof java.util.Date) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
        }
        else {
          pstm.setObject(i + 1, parameter);
        }
      }
      iResult = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
    }
    return iResult > 0;
  }
  
  public static
  boolean update(Connection conn, String sTable, Map<String,Object> mapValues, String sWhere, Object... parameters)
    throws Exception
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return false;
    }
    
    List<String> listFields = getFields(mapValues);
    if(listFields == null || listFields.size() == 0) {
      return false;
    }
    
    String sSQL = "UPDATE " + sTable + " SET ";
    String sFields = "";
    for(int i = 0; i < listFields.size(); i++) {
      sFields += "," + listFields.get(i) + "=?";
    }
    sSQL += sFields.substring(1);
    if(sWhere != null && sWhere.length() > 0) {
      sSQL += " WHERE " + sWhere;
    }
    
    int iResult = 0;
    int i = 0; 
    PreparedStatement pstm = null;
    try {
      pstm = conn.prepareStatement(sSQL);
      for(i = 0; i < listFields.size(); i++) {
        String sField = listFields.get(i);
        Object parameter = mapValues.get(sField);
        if(parameter instanceof Integer) {
          pstm.setInt(i + 1, ((Integer) parameter).intValue());
        }
        else if(parameter instanceof Long) {
          pstm.setLong(i + 1, ((Long) parameter).longValue());
        }
        else if(parameter instanceof Double) {
          pstm.setDouble(i + 1, ((Double) parameter).doubleValue());
        }
        else if(parameter instanceof String) {
          pstm.setString(i + 1, (String) parameter);
        }
        else if(parameter instanceof Boolean) {
          pstm.setInt(i + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
        }
        else if(parameter instanceof java.util.Calendar) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
        }
        else if(parameter instanceof java.sql.Date) {
          pstm.setDate(i + 1, (java.sql.Date) parameter);
        }
        else if(parameter instanceof java.sql.Timestamp) {
          pstm.setTimestamp(i + 1, (java.sql.Timestamp) parameter);
        }
        else if(parameter instanceof java.sql.Time) {
          pstm.setTime(i + 1, (java.sql.Time) parameter);
        }
        else if(parameter instanceof java.util.Date) {
          pstm.setDate(i + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
        }
        else {
          pstm.setObject(i + 1, parameter);
        }
      }
      setParameters(pstm, i, parameters);
      iResult = pstm.executeUpdate();
    }
    finally {
      if(pstm != null) try { pstm.close(); } catch(Exception ex) {}
    }
    return iResult > 0;
  }
  
  @SuppressWarnings("unchecked")
  public static
  List<String> getFields(Map<String,Object> mapValues)
  {
    if(mapValues == null || mapValues.isEmpty()) {
      return new ArrayList<String>(0);
    }
    
    Object result = mapValues.get(sFIELDS);
    if(result instanceof List) {
      return  (List<String>) result;
    }
    else if(result instanceof String) {
      String sFields = (String) result;
      if(sFields.length() > 0) {
        if(sFields.startsWith("[") && sFields.endsWith("]")) {
          sFields = sFields.substring(1, sFields.length()-1);
        }
        List<String> list = new ArrayList<String>();
        int iIndexOf = 0;
        int iBegin   = 0;
        iIndexOf     = sFields.indexOf(',');
        while(iIndexOf >= 0) {
          list.add(sFields.substring(iBegin, iIndexOf));
          iBegin = iIndexOf + 1;
          iIndexOf = sFields.indexOf(',', iBegin);
        }
        list.add(sFields.substring(iBegin));
        return list;
      }
    }
    
    List<String> listResult = new ArrayList<String>();
    Iterator<String> iterator = mapValues.keySet().iterator();
    while(iterator.hasNext()) {
      listResult.add(iterator.next());
    }
    return listResult;
  }
  
  public static
  String buildInSet(Collection<Object> items)
  {
    if(items == null || items.size() == 0) {
      return "";
    }
    
    StringBuilder sb = new StringBuilder();
    Iterator<Object> iterator = items.iterator();
    while(iterator.hasNext()) {
      Object item = iterator.next();
      if(item instanceof String) {
        sb.append(",'" +((String) item).replace("'", "''") + "'");
      }
      else {
        sb.append("," + item);
      }
    }
    
    return sb.toString().substring(1);
  }
  
  public static
  String buildInSet(Collection<Object> items, int max)
  {
    if(items == null || items.size() == 0) {
      return "";
    }
    
    int count = 0;
    StringBuilder sb = new StringBuilder();
    Iterator<Object> iterator = items.iterator();
    while(iterator.hasNext()) {
      Object item = iterator.next();
      if(item instanceof String) {
        sb.append(",'" +((String) item).replace("'", "''") + "'");
      } 
      else {
        sb.append("," + item);
      }
      
      count++;
      if(count >= max) break;
    }
    
    return sb.toString().substring(1);
  }
  
  @SuppressWarnings("unchecked")
  public static
  String buildInSet(List<Object> items, String sSymbolic)
  {
    if(items == null || items.size() == 0) {
      return "";
    }
    
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < items.size(); i++) {
      Object item = items.get(i);
      if(item instanceof String) {
        sb.append(",'" +((String) item).replace("'", "''") + "'");
      } 
      else if(item instanceof Map) {
        Object oValue = ((Map<String,Object>) item).get(sSymbolic);
        if(oValue == null) {
          continue;
        }
        if(oValue instanceof String) {
          sb.append(",'" + oValue.toString().replace("'", "''") + "'");
        } else {
          sb.append("," + oValue);
        }
      }
      else {
        sb.append("," + item);
      }
    }
    
    return sb.toString().substring(1);
  }
  
  public static
  String buildInSet(int items)
  {
    if(items < 1) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < items; i++) sb.append(",?");
    return sb.substring(1);
  }
  
  public static
  byte[] getBLOBContent(ResultSet rs, int index)
    throws Exception
  {
    Blob blob = rs.getBlob(index);
    if(blob == null) {
      return null;
    }
    
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
  byte[] getBLOBContent(ResultSet rs, String sFieldName)
    throws Exception
  {
    Blob blob = rs.getBlob(sFieldName);
    if(blob == null) {
      return null;
    }
    
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
  boolean setEmptyBLOB(Connection conn, String sField, String sTable, String sWhere)
    throws Exception
  {
    int iRows = 0;
    String sSQL = "UPDATE " + sTable + " SET " + sField + "=EMPTY_BLOB()";
    if(sWhere != null && sWhere.length() > 0) {
      sSQL += " WHERE " + sWhere;
    }
    Statement stm = null;
    try{
      stm = conn.createStatement();
      iRows = stm.executeUpdate(sSQL);
    }
    finally {
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
    return iRows > 0;
  }
  
  public static
  boolean setBLOBContent(Connection conn, String sField, String sTable, String sWhere, byte[] abBlobContent)
    throws Exception
  {
    if(abBlobContent == null || abBlobContent.length == 0) {
      return setEmptyBLOB(conn, sField, sTable, sWhere);
    }
    String sSQL = "SELECT " + sField + " FROM " + sTable;
    if(sWhere != null && sWhere.length() > 0) sSQL += " WHERE " + sWhere;
    sSQL += " FOR UPDATE";
    boolean boResult = false;
    Statement stm = null;
    ResultSet rs = null;
    try{
      stm = conn.createStatement();
      rs = stm.executeQuery(sSQL);
      if(rs.next()) {
        Blob blob = rs.getBlob(sField);
        OutputStream blobOutputStream = blob.setBinaryStream(0);
        for(int i = 0; i < abBlobContent.length; i++) {
          blobOutputStream.write(abBlobContent[i]);
        }
        blobOutputStream.flush();
        blobOutputStream.close();
        boResult = true;
      }
    }
    finally {
      if(rs  != null) try{ rs.close();  } catch(Exception ex) {}
      if(stm != null) try{ stm.close(); } catch(Exception ex) {}
    }
    return boResult;
  }
  
  public static
  void setParameters(PreparedStatement pstm, int start, Object... parameters)
    throws Exception
  {
    if(parameters == null || parameters.length == 0) {
      return;
    }
    
    for(int i = 0; i < parameters.length; i++) {
      Object parameter = parameters[i];
      if(parameter instanceof Integer) {
        pstm.setInt(i + start + 1, ((Integer) parameter).intValue());
      }
      else if(parameter instanceof Long) {
        pstm.setLong(i + start +  1, ((Long) parameter).longValue());
      }
      else if(parameter instanceof Double) {
        pstm.setDouble(i + start + 1, ((Double) parameter).doubleValue());
      }
      else if(parameter instanceof String) {
        pstm.setString(i + start + 1, (String) parameter);
      }
      else if(parameter instanceof Boolean) {
        pstm.setInt(i + start + 1, ((Boolean) parameter).booleanValue() ? 1 : 0);
      }
      else if(parameter instanceof java.util.Calendar) {
        pstm.setDate(i + start + 1, new java.sql.Date(((java.util.Calendar) parameter).getTimeInMillis()));
      }
      else if(parameter instanceof java.sql.Date) {
        pstm.setDate(i + start + 1, (java.sql.Date) parameter);
      }
      else if(parameter instanceof java.sql.Timestamp) {
        pstm.setTimestamp(i + start + 1, (java.sql.Timestamp) parameter);
      }
      else if(parameter instanceof java.sql.Time) {
        pstm.setTime(i + start + 1, (java.sql.Time) parameter);
      }
      else if(parameter instanceof java.util.Date) {
        pstm.setDate(i + start + 1, new java.sql.Date(((java.util.Date) parameter).getTime()));
      }
      else {
        pstm.setObject(i + start + 1, parameter);
      }
    }
  }
}
