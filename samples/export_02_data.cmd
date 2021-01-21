@echo off

"%JAVA_HOME%"\bin\java -cp .;dbsql-tools-1.0.0.jar;ojdbc6.jar org.dew.dbsql.ExportData db_src hsqldb