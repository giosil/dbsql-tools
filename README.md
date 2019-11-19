# DBSQL-Tools

A set of tools for manage relational SQL databases.

## Tools available

- `java org.dew.dbsql.CommandSQL data_source`
- `java org.dew.dbsql.ExecuteSQL data_source sql_file`
- `java org.dew.dbsql.ExportData data_source`
- `java org.dew.dbsql.ExportSchema data_source`
- `java org.dew.dbsql.ViewSchema data_source`

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
