#!/bin/bash

$JAVA_HOME/bin/java -cp .:dbsql-tools-1.0.0.jar:ojdbc6.jar org.dew.dbsql.ExportSchema db_src hsqldb