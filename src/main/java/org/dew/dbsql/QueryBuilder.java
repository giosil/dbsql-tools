package org.dew.dbsql;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public
class QueryBuilder
{
  public final static String NULL = "null";
  
  // static options
  protected static boolean TO_UPPER    = false;
  protected static String  DBMS        = "sql";
  protected static Object  TRUE_VALUE  = 1;
  protected static Object  FALSE_VALUE = 0;
  
  // instance options
  protected boolean toUpper    = TO_UPPER;
  protected String  dbms       = DBMS;
  protected Object  trueValue  = TRUE_VALUE;
  protected Object  falseValue = FALSE_VALUE;
  
  // state
  protected Map<String, Object> mapValues;
  protected List<String> listFields;
  protected List<String> listPresCase;
  
  public
  QueryBuilder()
  {
    init();
  }
  
  public static
  void setToUpperRef(boolean boToUpper)
  {
    TO_UPPER = boToUpper;
  }
  
  public static
  void setDBMSRef(String dbms)
  {
    if(dbms == null || dbms.length() == 0) {
      dbms = "sql";
    }
    else {
      dbms = dbms.trim().toLowerCase();
    }
    DBMS = dbms;
  }
  
  public static
  String detectcDBMS(Connection conn)
  {
    if(conn == null) return null;
    
    String result = null;
    
    // Retrieve url...
    String url = null;
    try {
      DatabaseMetaData metadata = conn.getMetaData();
      if(metadata != null) url = metadata.getURL();
    }
    catch(Exception ex) {
    }
    if(url == null || url.length() == 0) {
      return "sql";
    }
    
    // Retrieve dbms...
    int sep = url.indexOf(':', 5);
    if(sep > 0) {
      result = url.substring(5, sep).toLowerCase();
    }
    if(result == null || result.length() == 0) {
      result = "sql";
    }
    
    return result;
  }
  
  public static
  void setDBMSRef(Connection conn)
  {
    if(conn == null) return;
    
    setDBMSRef(detectcDBMS(conn));
  }
  
  public static
  void setTrueValueRef(Object trueValue)
  {
    TRUE_VALUE = trueValue;
    if(TRUE_VALUE == null) TRUE_VALUE = 1;
  }
  
  public static
  void setFalseValueRef(Object falseValue)
  {
    FALSE_VALUE = falseValue;
    if(FALSE_VALUE == null) FALSE_VALUE = 0;
  }
  
  public
  void setToUpper(boolean boToUpper)
  {
    this.toUpper = boToUpper;
  }
  
  public
  void setDBMS(String dbms)
  {
    if(dbms == null || dbms.length() == 0) {
      dbms = "sql";
    }
    else {
      dbms = dbms.trim().toLowerCase();
    }
    this.dbms = dbms;
  }
  
  public
  void setDBMS(Connection conn)
  {
    if(conn == null) return;
    
    setDBMS(detectcDBMS(conn));
  }
  
  public
  void setTrueValue(Object trueValue)
  {
    if(trueValue == null) trueValue = TRUE_VALUE;
    this.trueValue = trueValue;
  }
  
  public
  void setFalseValue(Object falseValue)
  {
    if(falseValue == null) falseValue = FALSE_VALUE;
    this.falseValue = falseValue;
  }
  
  public
  void preserveCaseFor(Object fields)
  {
    if(fields instanceof String) {
      String sFields = (String) fields;
      if(sFields.indexOf(',') >= 0) {
        listPresCase = stringToList(sFields);
      }
      else {
        listPresCase = new ArrayList<String>();
        listPresCase.add((String) sFields);
      }
    }
    else {
      listPresCase = toListOfString(fields);
    }
  }
  
  public
  boolean isToUpper()
  {
    return toUpper;
  }
  
  public
  void init()
  {
    mapValues    = new HashMap<String, Object>();
    listFields   = new ArrayList<String>();
    listPresCase = new ArrayList<String>();
  }
  
  public
  String insert(String table)
  {
    return insert(table, false);
  }
  
  public
  String insert(String table, boolean boWithParameters)
  {
    if(listFields == null || listFields.isEmpty()) return "";
    
    StringBuilder sbSQL = new StringBuilder();
    
    String sFieldValues = null;
    if(boWithParameters) {
      sFieldValues = getQuestionPoints();
    }
    else {
      StringBuilder fieldValues = new StringBuilder();
      for(int i = 0; i < listFields.size(); i++) {
        String key   = listFields.get(i);
        String value = null;
        Object vtemp = mapValues.get(key);
        
        if(vtemp instanceof String) {
          if(vtemp.equals(NULL)) {
            value = "NULL";
          }
          else {
            if(toUpper && !listPresCase.contains(key)) {
              value = "'" + doubleQuotes(vtemp.toString().toUpperCase()) + "'";
            }
            else {
              value = "'" + doubleQuotes(vtemp.toString()) + "'";
            }
          }
        }
        else if(vtemp instanceof Calendar) {
          value = toString((Calendar) vtemp);
        }
        else if(vtemp instanceof Date) {
          value = toString((Date) vtemp);
        }
        else if(vtemp instanceof Boolean) {
          value = decodeBoolean((Boolean) vtemp);
        }
        else {
          value = vtemp.toString();
        }
        
        fieldValues.append(value);
        fieldValues.append(",");
      }
      
      sFieldValues = fieldValues.toString();
      if(sFieldValues.length() > 0) {
        sFieldValues = sFieldValues.substring(0, sFieldValues.length() - 1);
      }
    }
    
    sbSQL.append("INSERT INTO ");
    sbSQL.append(table);
    sbSQL.append('(');
    sbSQL.append(getFields());
    sbSQL.append(')');
    
    sbSQL.append(" VALUES(");
    sbSQL.append(sFieldValues);
    sbSQL.append(')');
    
    return sbSQL.toString();
  }
  
  public
  String update(String table)
  {
    return update(table, false);
  }
  
  public
  String update(String table, boolean boWithParameters)
  {
    if(listFields == null || listFields.isEmpty()) return "";
    
    StringBuilder fields = new StringBuilder();
    StringBuilder sbSQL  = new StringBuilder();
    
    for(int i = 0; i < listFields.size(); i++) {
      String key = listFields.get(i);
      
      String value = null;
      if(boWithParameters) {
        value = "?";
      }
      else {
        Object vtemp = mapValues.get(key);
        
        if(vtemp instanceof String) {
          if(vtemp.equals(NULL)) {
            value = "NULL";
          }
          else {
            if(toUpper && !listPresCase.contains(key)) {
              value = "'" + doubleQuotes(vtemp.toString().toUpperCase()) + "'";
            }
            else {
              value = "'" + doubleQuotes(vtemp.toString()) + "'";
            }
          }
        }
        else if(vtemp instanceof Calendar) {
          value = toString((Calendar) vtemp);
        }
        else if(vtemp instanceof Date) {
          value = toString((Date) vtemp);
        }
        else if(vtemp instanceof Boolean) {
          value = decodeBoolean((Boolean) vtemp);
        }
        else {
          value = vtemp.toString();
        }
      }
      
      fields.append(key);
      fields.append('=');
      fields.append(value);
      fields.append(",");
    }
    
    String sField = fields.toString();
    if(sField.length() > 0) {
      sField = sField.substring(0, sField.length() - 1);
    }
    
    sbSQL.append("UPDATE ");
    sbSQL.append(table);
    sbSQL.append(" SET ");
    sbSQL.append(sField);
    sbSQL.append(" ");
    
    return sbSQL.toString();
  }
  
  public
  String select(String tables)
  {
    return select(tables, null, null, false);
  }
  
  public
  String select(String tables, boolean boDistinct)
  {
    return select(tables, null, null, boDistinct);
  }
  
  public
  String select(String tables, Map<String, Object> htFilter)
  {
    return select(tables, htFilter, null, false);
  }
  
  public
  String select(String tables, Map<String, Object> htFilter, String sAdditionalClause)
  {
    return select(tables, htFilter, sAdditionalClause, false);
  }
  
  public
  String select(String tables, Map<String, Object> htFilter, String sAdditionalClause, boolean boDistinct)
  {
    if(listFields == null || listFields.isEmpty()) return "";
    
    StringBuilder sbSQL = new StringBuilder();
    if(boDistinct) {
      sbSQL.append("SELECT DISTINCT ");
    }
    else {
      sbSQL.append("SELECT ");
    }
    sbSQL.append(getFields());
    sbSQL.append(" FROM " + tables + " ");
    
    if(htFilter != null) {
      StringBuilder sbWhere = new StringBuilder();
      for(int i = 0; i < listFields.size(); i++) {
        String sField = listFields.get(i).toString();
        
        String value = null;
        Object okey  = mapValues.get(sField);
        if(okey == null) continue;
        
        boolean boStartsWithPerc = false;
        boolean boEndsWithPerc   = false;
        if(okey instanceof String) {
          String sKey = (String) okey;
          boStartsWithPerc = sKey.startsWith("%");
          if(boStartsWithPerc) sKey = sKey.substring(1);
          boEndsWithPerc = sKey.endsWith("%");
          if(boEndsWithPerc) sKey = sKey.substring(0, sKey.length() - 1);
          okey = sKey;
        }
        
        boolean boLike = false;
        Object vtemp = htFilter.get(okey);
        
        if(vtemp == null) {
          continue;
        }
        else if(vtemp instanceof String) {
          if(((String) vtemp).trim().length() == 0) {
            continue;
          }
          else if(vtemp.equals(NULL)) {
            value = "NULL";
          }
          else {
            if(toUpper && !listPresCase.contains(okey)) {
              value = "'";
              if(boStartsWithPerc) value += "%";
              value += doubleQuotes(vtemp.toString().toUpperCase());
              if(boEndsWithPerc) value += "%";
              value += "'";
            }
            else {
              value = "'";
              if(boStartsWithPerc) value += "%";
              value += doubleQuotes(vtemp.toString());
              if(boEndsWithPerc) value += "%";
              value += "'";
            }
          }
          
          boLike = value.indexOf('%') >= 0;
        }
        else if(vtemp instanceof Calendar) {
          value = toString((Calendar) vtemp);
        }
        else if(vtemp instanceof Date) {
          value = toString((Date) vtemp);
        }
        else if(vtemp instanceof Boolean) {
          value = decodeBoolean((Boolean) vtemp);
        }
        else {
          value = vtemp.toString();
        }
        
        String sFieldName = null;
        int iSepFieldAlias = sField.indexOf(' ');
        if(iSepFieldAlias > 0) {
          sFieldName = sField.substring(0, iSepFieldAlias);
        }
        else {
          sFieldName = sField;
        }
        
        sbWhere.append(sFieldName);
        if(boLike) {
          sbWhere.append(" LIKE ");
        }
        else {
          sbWhere.append('=');
        }
        sbWhere.append(value);
        sbWhere.append(" AND ");
      }
      
      String sWhereClause = sbWhere.toString();
      if(sWhereClause.length() > 0) {
        sWhereClause = sWhereClause.substring(0, sWhereClause.length() - 5);
        sbSQL.append("WHERE ");
        sbSQL.append(sWhereClause);
        if(sAdditionalClause != null && sAdditionalClause.trim().length() > 0) {
          sbSQL.append(" AND " + sAdditionalClause);
        }
      }
      else {
        if(sAdditionalClause != null && sAdditionalClause.trim().length() > 0) {
          sbSQL.append("WHERE " + sAdditionalClause);
        }
      }
    }
    
    return sbSQL.toString();
  }
  
  public
  void add(String field)
  {
    if(!listFields.contains(field)) {
      listFields.add(field);
    }
  }
  
  public
  void put(String field, Object value)
  {
    if(value == null) {
      mapValues.put(field, NULL);
    }
    else {
      mapValues.put(field, value);
    }
    
    if(!listFields.contains(field)) {
      listFields.add(field);
    }
  }
  
  public
  void put(String field, Object value, Object defaultValue)
  {
    if(value == null) {
      if(defaultValue == null) {
        mapValues.put(field, NULL);
      }
      else {
        mapValues.put(field, defaultValue);
      }
    }
    else {
      mapValues.put(field, value);
    }
    
    if(!listFields.contains(field)) {
      listFields.add(field);
    }
  }
  
  protected
  String getFields()
  {
    if(listFields == null) return "";
    
    String sResult = "";
    for(int i = 0; i < listFields.size(); i++) {
      sResult += listFields.get(i) + ",";
    }
    if(sResult.length() > 0) {
      sResult = sResult.substring(0, sResult.length() - 1);
    }
    return sResult;
  }
  
  protected
  String getQuestionPoints()
  {
    if(listFields == null) return "";
    
    String sResult = "";
    for(int i = 0; i < listFields.size(); i++) {
      sResult += "?,";
    }
    if(sResult.length() > 0) {
      sResult = sResult.substring(0, sResult.length() - 1);
    }
    return sResult;
  }
  
  public
  String decodeBoolean(Boolean value)
  {
    if(value == null) return "NULL";
    
    if(trueValue instanceof Number) {
      if(value.booleanValue()) {
        return trueValue.toString();
      }
      else {
        return falseValue.toString();
      }
    }
    else {
      if(value.booleanValue()) {
        return "'" + trueValue.toString() + "'";
      }
      else {
        return "'" + falseValue.toString() + "'";
      }
    }
  }
  
  public
  String decodeBoolean(boolean value)
  {
    if(trueValue instanceof Number) {
      if(value) {
        return trueValue.toString();
      }
      else {
        return falseValue.toString();
      }
    }
    else {
      if(value) {
        return "'" + trueValue.toString() + "'";
      }
      else {
        return "'" + falseValue.toString() + "'";
      }
    }
  }
  
  public static
  String decodeBooleanRef(Boolean value)
  {
    if(value == null) return "NULL";
    
    if(TRUE_VALUE instanceof Number) {
      if(value.booleanValue()) {
        return TRUE_VALUE.toString();
      }
      else {
        return FALSE_VALUE.toString();
      }
    }
    else {
      if(value.booleanValue()) {
        return "'" + TRUE_VALUE.toString() + "'";
      }
      else {
        return "'" + FALSE_VALUE.toString() + "'";
      }
    }
  }
  
  public static
  String decodeBooleanRef(boolean value)
  {
    if(TRUE_VALUE instanceof Number) {
      if(value) {
        return TRUE_VALUE.toString();
      }
      else {
        return FALSE_VALUE.toString();
      }
    }
    else {
      if(value) {
        return "'" + TRUE_VALUE.toString() + "'";
      }
      else {
        return "'" + FALSE_VALUE.toString() + "'";
      }
    }
  }
  
  public static
  Object getBooleanValue(Object oValue)
  {
    if(oValue == null) return null;
    boolean value = toBoolean(oValue, false);
    if(value) {
      return TRUE_VALUE;
    }
    else {
      return FALSE_VALUE;
    }
  }
  
  public
  String toString(Calendar cal)
  {
    if(cal == null) return "NULL";
    
    int iYear   = cal.get(java.util.Calendar.YEAR);
    int iMonth  = cal.get(java.util.Calendar.MONTH) + 1;
    int iDay    = cal.get(java.util.Calendar.DAY_OF_MONTH);
    int iHour   = cal.get(Calendar.HOUR_OF_DAY);
    int iMinute = cal.get(Calendar.MINUTE);
    int iSecond = cal.get(Calendar.SECOND);
    String sMonth  = iMonth  < 10 ? "0" + iMonth  : String.valueOf(iMonth);
    String sDay    = iDay    < 10 ? "0" + iDay    : String.valueOf(iDay);
    String sHour   = iHour   < 10 ? "0" + iHour   : String.valueOf(iHour);
    String sMinute = iMinute < 10 ? "0" + iMinute : String.valueOf(iMinute);
    String sSecond = iSecond < 10 ? "0" + iSecond : String.valueOf(iSecond);
    if(dbms != null && dbms.indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "','YYYY-MM-DD HH24:MI:SS')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "'";
  }
  
  public static
  String toString(Calendar cal, String theDbms)
  {
    if(cal == null) return "NULL";
    
    int iYear   = cal.get(java.util.Calendar.YEAR);
    int iMonth  = cal.get(java.util.Calendar.MONTH) + 1;
    int iDay    = cal.get(java.util.Calendar.DAY_OF_MONTH);
    int iHour   = cal.get(Calendar.HOUR_OF_DAY);
    int iMinute = cal.get(Calendar.MINUTE);
    int iSecond = cal.get(Calendar.SECOND);
    String sMonth  = iMonth  < 10 ? "0" + iMonth  : String.valueOf(iMonth);
    String sDay    = iDay    < 10 ? "0" + iDay    : String.valueOf(iDay);
    String sHour   = iHour   < 10 ? "0" + iHour   : String.valueOf(iHour);
    String sMinute = iMinute < 10 ? "0" + iMinute : String.valueOf(iMinute);
    String sSecond = iSecond < 10 ? "0" + iSecond : String.valueOf(iSecond);
    if(theDbms != null && theDbms.toLowerCase().indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "','YYYY-MM-DD HH24:MI:SS')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMinute + ":" + sSecond + "'";
  }
  
  public
  String toString(Date date)
  {
    if(date == null) return "NULL";
    
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int iYear  = cal.get(Calendar.YEAR);
    int iMonth = cal.get(Calendar.MONTH) + 1;
    int iDay   = cal.get(Calendar.DAY_OF_MONTH);
    String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
    String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
    if(dbms != null && dbms.indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + "'";
  }
  
  public static 
  String toString(Date date, String theDbms)
  {
    if(date == null) return "NULL";
    
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int iYear  = cal.get(Calendar.YEAR);
    int iMonth = cal.get(Calendar.MONTH) + 1;
    int iDay   = cal.get(Calendar.DAY_OF_MONTH);
    String sMonth = iMonth < 10 ? "0" + iMonth : String.valueOf(iMonth);
    String sDay   = iDay   < 10 ? "0" + iDay   : String.valueOf(iDay);
    if(theDbms != null && theDbms.toLowerCase().indexOf("oracle") >= 0) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + "'";
  }
  
  public static
  String doubleQuotes(String text)
  {
    StringBuilder result = new StringBuilder(text.length());
    char c;
    for(int i = 0; i < text.length(); i++) {
      c = text.charAt(i);
      if(c == '\'') result.append('\'');
      result.append(c);
    }
    return result.toString();
  }
  
  public static
  List<String> toListOfString(Object object)
  {
    if(object == null) {
      return new ArrayList<String>();
    }
    if(object instanceof Collection) {
      Collection<?> collection = (Collection<?>) object;
      ArrayList<String> arrayList = new ArrayList<String>(collection.size());
      Iterator<?> iterator = collection.iterator();
      while(iterator.hasNext()) {
        Object item = iterator.next();
        if(item == null) continue;
        arrayList.add(item.toString());
      }
      return arrayList;
    }
    if(object.getClass().isArray()) {
      int length = Array.getLength(object);
      ArrayList<String> arrayList = new ArrayList<String>(length);
      for(int i = 0; i < length; i++) {
        Object item = Array.get(object, i);
        if(item == null) continue;
        arrayList.add(item.toString());
      }
      return arrayList;
    }
    if(object instanceof String) {
      return stringToList((String) object);
    }
    ArrayList<String> arrayList = new ArrayList<String>(1);
    arrayList.add(object.toString());
    return arrayList;
  }
  
  public static
  List<String> stringToList(String sText)
  {
    if(sText == null || sText.length() == 0) {
      return new ArrayList<String>(0);
    }
    if(sText.startsWith("[") && sText.endsWith("]")) {
      sText = sText.substring(1, sText.length()-1);
    }
    ArrayList<String> arrayList = new ArrayList<String>();
    int iIndexOf = 0;
    int iBegin   = 0;
    iIndexOf     = sText.indexOf(',');
    while(iIndexOf >= 0) {
      arrayList.add(sText.substring(iBegin, iIndexOf));
      iBegin = iIndexOf + 1;
      iIndexOf = sText.indexOf(',', iBegin);
    }
    arrayList.add(sText.substring(iBegin));
    return arrayList;
  }
  
  public static
  boolean toBoolean(Object object, boolean bDefault)
  {
    if(object == null) return bDefault;
    if(object instanceof Boolean) {
      return ((Boolean) object).booleanValue();
    }
    if(object instanceof Number) {
      return ((Number) object).intValue() != 0;
    }
    String sValue = object.toString().trim();
    if(sValue.length() == 0) return bDefault;
    char c0 = sValue.charAt(0);
    return "YySsTt1Jj".indexOf(c0) >= 0;
  }
}
