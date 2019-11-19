package org.dew.dbsql;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.sql.*;

public
class JdbcDataSource
{
	public static Properties config = new Properties();
	
	static {
		InputStream is = null;
		try {
			URL urlCfg = Thread.currentThread().getContextClassLoader().getResource("jdbc.cfg");
			if(urlCfg != null) {
				is = urlCfg.openStream();
				config.load(is);
			}
			else {
				System.out.println("GRAVE: jdbc.cfg not found in classpath.");
			}
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		finally {
			try { is.close(); } catch (Exception ex) {}
		}
	}
	
	public static
	String getUser(String sName)
		throws Exception
	{
		return config.getProperty(sName + ".user");
	}
	
	public static
	Connection getConnection(String sName)
		throws Exception
	{
		String sDriver = config.getProperty(sName + ".driver");
		if(sDriver == null || sDriver.length() == 0) return null;
		Class.forName(sDriver);
		
		String sURL  = config.getProperty(sName + ".url");
		if(sURL == null || sURL.length() == 0) return null;
		String sUser = config.getProperty(sName + ".user");
		String sPass = config.getProperty(sName + ".password");
		
		System.out.println("Create connection at " + sURL + " with " + sUser + " (autocommit=false)...");
		
		Connection conn = DriverManager.getConnection(sURL, sUser, sPass);
		conn.setAutoCommit(false);
		return conn;
	}
}
