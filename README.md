# DBSQL-Tools

A set of tools to manage relational SQL databases.

## Tools available

- `java org.dew.dbsql.CommandSQL data_source`
- `java org.dew.dbsql.ExecuteSQL data_source sql_file`
- `java org.dew.dbsql.ExportData data_source`
- `java org.dew.dbsql.ExportSchema data_source`
- `java org.dew.dbsql.ViewSchema data_source`

## Utilities available

**DB** helper class:

```java
Connection conn = null;
try {
  Context ctx = new InitialContext();
  DataSource ds = (DataSource) ctx.lookup("java:/jdbc/db_test");
  conn = ds.getConnection();
  
  int count = DB.readInt(conn, "SELECT COUNT(*) FROM CONTACTS WHERE NAME=? AND AGE>?", "CLARK", 40);
  
  List<String> names = DB.readListOfString(conn, "SELECT NAME FROM CONTACTS WHERE AGE>? ORDER BY NAME", 40);
}
catch(Exception ex) {
  ex.printStackTrace();
}
finally {
  if(conn != null) try { conn.close(); } catch (Exception e) {}
}

```
**QueryBuilder** helper class:

```java

Map<String, Object> mapFilter = new HashMap<String, Object>();
mapFilter.put("d", "ADMIN%");
  
QueryBuilder qb = new QueryBuilder();
qb.put("ID_ROLE",     "i");
qb.put("DESCRIPTION", "d");
qb.put("ENABLED",     "e");
  
String sAddClause = "ENABLED=" + qb.decodeBoolean(true);
  
String sSQL = qb.select("ADM_ROLES", mapFilter, sAddClause);
  
sSQL += " ORDER BY ID_ROLE";
```

## Data sources configuration (jdbc.cfg in CLASSPATH)

```
# Oracle Database
db_orcl.driver    = oracle.jdbc.driver.OracleDriver
db_orcl.url       = jdbc:oracle:thin:@localhost:1521:orcl
db_orcl.user      = test
db_orcl.password  = test

# MySQL Database
db_mysql.driver   = com.mysql.jdbc.Driver
db_mysql.url      = jdbc:mysql://localhost:3306/test
db_mysql.user     = test
db_mysql.password = test
```

## Build

- `git clone https://github.com/giosil/dbsql-tools.git`
- `mvn clean install`

## Contributors

* [Giorgio Silvestris](https://github.com/giosil)
