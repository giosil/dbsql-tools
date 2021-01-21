#!/bin/bash

$JAVA_HOME/bin/java -cp .:dbsql-tools-1.0.0.jar:h2.jar org.dew.dbsql.ExecuteSQL db_dst TEST_FK.sql