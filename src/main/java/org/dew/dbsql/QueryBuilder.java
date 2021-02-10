package org.dew.dbsql;

import java.lang.reflect.Array;

import java.util.*;

public
class QueryBuilder
{
  public final static String NULL = "null";
  
  protected Map<String, Object> mapValues;
  protected List<String> listFields;
  protected List<String> listPresCase;
  
  protected boolean toUpper = false;
  protected boolean oracle  = false;
  
  public
  QueryBuilder()
  {
    init();
  }
  
  public
  void setToUpper(boolean boToUpper)
  {
    this.toUpper = boToUpper;
  }
  
  public
  void setOracle(boolean oracle)
  {
    this.oracle = oracle;
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
    listFields   = new Vector<String>();
    listPresCase = new Vector<String>();
  }
  
  public
  String insert(String table)
  {
    return insert(table, false);
  }
  
  public
  String insert(String table, boolean boWithParameters)
  {
    if(listFields.isEmpty()) return "";
    
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
  String update(String table,  boolean boWithParameters)
  {
    if(listFields.isEmpty()) return "";
    
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
    if(listFields.isEmpty()) return "";
    
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
        
        sbWhere.append(sField);
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
        if(sAdditionalClause != null &&
            sAdditionalClause.trim().length() > 0) {
          sbSQL.append(" AND " + sAdditionalClause);
        }
      }
      else {
        if(sAdditionalClause != null &&
            sAdditionalClause.trim().length() > 0) {
          sbSQL.append("WHERE " + sAdditionalClause);
        }
      }
    }
    
    return sbSQL.toString();
  }
  
  public
  void put(String key, Object value)
  {
    if(value == null) {
      mapValues.put(key, NULL);
    }
    else {
      mapValues.put(key, value);
    }
    
    if(!listFields.contains(key)) {
      listFields.add(key);
    }
  }
  
  public
  void add(String sField)
  {
    if(!listFields.contains(sField)) {
      listFields.add(sField);
    }
  }
  
  public
  void put(String key, Object value, Object defaultValue)
  {
    if(value == null) {
      if(defaultValue == null) {
        mapValues.put(key, NULL);
      }
      else {
        mapValues.put(key, defaultValue);
      }
    }
    else {
      mapValues.put(key, value);
    }
    
    if(!listFields.contains(key)) {
      listFields.add(key);
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
  String decodeBoolean(Boolean value)
  {
    if(value == null) return "";
    if(value.booleanValue()) return "1";
    return "0";
  }
  
  public static
  Integer decodeBooleanInteger(Boolean value)
  {
    if(value == null) return null;
    if(value.booleanValue()) return 1;
    return 0;
  }
  
  public static
  String decodeBoolean(boolean value)
  {
    return value ? "1" : "0";
  }
  
  public static
  int decodeBooleanInt(boolean value)
  {
    return value ? 1 : 0;
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
    if(oracle) {
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
    if(oracle) {
      return "TO_DATE('" + iYear + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";
    }
    return "'" + iYear + "-" + sMonth + "-" + sDay + "'";
  }
}
