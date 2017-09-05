/*
 Copyright (c) 2017 onghuilam@gmail.com
 
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 The Software shall be used for Good, not Evil.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 
 */

package hl.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

public class JdbcDBMgr {

	public static String KEY_DB_CLASSNAME 	= "db.jdbc.classname";
	public static String KEY_DB_URL 		= "db.url";
	public static String KEY_DB_UID 		= "db.uid";
	public static String KEY_DB_PWD			= "db.pwd";
	public static String KEY_DB_CONNPOOL	= "db.pool.size";
	
	public String db_classname 		= null;
	public String db_url 			= null;
	public String db_uid 			= null;
	public String db_pwd 			= null;
	public int db_conn_pool_size	= 2;
	
	private static Stack<Connection> stackConns = new Stack<Connection>();
	private Map<String, String> mapSQLtemplate 	= new HashMap<String, String>();
	private static List<String> listNumericType = null;
	private static List<String> listDoubleType 	= null;
	private static List<String> listBooleanType = null;
		
	static{
		listNumericType = new ArrayList<String>();
		listNumericType.add(int.class.getSimpleName());
		listNumericType.add(Integer.class.getSimpleName());
		listNumericType.add(long.class.getSimpleName());
		listNumericType.add(Long.class.getSimpleName());

		listDoubleType = new ArrayList<String>();
		listDoubleType.add(double.class.getSimpleName());
		listDoubleType.add(Double.class.getSimpleName());
		listDoubleType.add(float.class.getSimpleName());
		listDoubleType.add(Float.class.getSimpleName());
		
		listBooleanType = new ArrayList<String>();
		listBooleanType.add(boolean.class.getSimpleName());
		listBooleanType.add(Boolean.class.getSimpleName());

	}
	
	public JdbcDBMgr(String aResourcePath) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		Properties prop = new Properties();
		InputStream in = null;
		try {
			
			File file = new File(aResourcePath);
			if(file.isFile())
			{
				in = new FileInputStream(file);
			}
			else
			{
				in = JdbcDBMgr.class.getResourceAsStream(aResourcePath);
			}
			prop.load(in);
		}
		finally
		{
			if(in!=null)
				in.close();
		}
		initDB(prop);
	}
	
	public void clearSQLtemplates()
	{
		mapSQLtemplate.clear();
	}
	
	public String getSQLtemplates(String aSQLTemplateName)
	{
		return mapSQLtemplate.get(aSQLTemplateName);
	}
	
	public void addSQLtemplates(String aSQLTemplateName, String aSQL)
	{
		mapSQLtemplate.put(aSQLTemplateName, aSQL);
	}	
	
	public JdbcDBMgr(Properties aProp) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		initDB(aProp);
	}
	
	public JdbcDBMgr(String aClassName, String aDBUrl, String aDBUid, String aDBPwd) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		Properties prop = new Properties();
		prop.put(KEY_DB_CLASSNAME, aClassName);
		prop.put(KEY_DB_URL, aDBUrl);
		prop.put(KEY_DB_UID, aDBUid);
		prop.put(KEY_DB_PWD, aDBPwd);
		initDB(prop);
	}
	
	public void setDBConnPoolSize(int aSize)
	{
		db_conn_pool_size = aSize;
	}
	
	public int getDBConnPoolSize()
	{
		return db_conn_pool_size;
	}

	private void initDB(Properties aProp) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		db_classname = aProp.getProperty(KEY_DB_CLASSNAME);
		db_url = aProp.getProperty(KEY_DB_URL);
		db_uid = aProp.getProperty(KEY_DB_UID);
		db_pwd = aProp.getProperty(KEY_DB_PWD);
		
		String sConnSize = aProp.getProperty(KEY_DB_CONNPOOL);
		if(sConnSize!=null)
		{
			try{
				db_conn_pool_size = Integer.parseInt(sConnSize);
			}
			catch(NumberFormatException ex){}
		}
		
		//Init JDBC class
		Class.forName (db_classname).newInstance();
		
		//Test connection
		Connection conn = null;
		
		try {
			conn = getConnection(false);
			
			//Init connection pool
			if(conn!=null && db_conn_pool_size>0)
			{
				for(int i=0; i<db_conn_pool_size; i++)
				{
					Connection connCache = getConnection(false);
					stackConns.push(connCache);
				}
			}
		}finally
		{
			closeQuietly(conn, null, null);
		}
		
	}
		
	public Connection getConnection() throws SQLException
	{
		return getConnection(true);
	}
	
	public Connection getConnection(boolean isGetFromConnPool) throws SQLException
	{
		Connection conn = null;
		
		if(isGetFromConnPool)
		{
			try{
				conn = stackConns.pop();
			}catch(EmptyStackException ex){}
		}
		
		if(conn==null)
		{
			conn = DriverManager.getConnection (db_url, db_uid, db_pwd);
		}
		return conn;
	}
	
	public static PreparedStatement setParams(PreparedStatement aStatement, List<Object> aParamList ) throws NumberFormatException, SQLException
	{
		return setParams(aStatement, aParamList.toArray(new Object[aParamList.size()]));
	}
	public static PreparedStatement setParams(PreparedStatement aStatement, Object[] aParams ) throws NumberFormatException, SQLException
	{
		if(aParams!=null && aParams.length>0 && aStatement!=null)
		{
			for(int i=0; i<aParams.length; i++)
			{
				Object param = aParams[i];
				
				String sParamClassName = param.getClass().getSimpleName();
				if(String.class.getSimpleName().equals(sParamClassName))
				{
					aStatement.setString(i+1, param.toString());
				}
				else if(listNumericType.contains(sParamClassName))
				{
					aStatement.setLong(i+1, Long.parseLong(param.toString()));
				}
				else if(Date.class.getSimpleName().equals(sParamClassName))
				{
					aStatement.setDate(i+1, (Date)param);
				}
				else if(Timestamp.class.getSimpleName().equals(sParamClassName))
				{
					aStatement.setTimestamp(i+1, (Timestamp)param);
				}
				else if(listDoubleType.contains(sParamClassName))
				{
					aStatement.setDouble(i+1, Double.parseDouble(param.toString()));
				}
				else if(listBooleanType.contains(sParamClassName))
				{
					aStatement.setBoolean(i+1, Boolean.parseBoolean(param.toString()));
				}
			}
		}		
		return aStatement;
	}

	public long executeUpdate(String aSQL, List<Object> aParamList) throws SQLException
	{
		return executeUpdate(aSQL, aParamList.toArray(new Object[aParamList.size()]));
	}
	public long executeUpdate(String aSQL, Object[] aParams ) throws SQLException
	{
		Connection conn 		= null;
		PreparedStatement stmt	= null;
		
		long lAffectedRows 		= 0;
		try{
			conn 	= getConnection();
			stmt 	= conn.prepareStatement(aSQL);
			stmt 	= setParams(stmt, aParams);
			
			lAffectedRows = stmt.executeUpdate();
		}finally
		{
			closeQuietly(conn, stmt, null);
		}
		return lAffectedRows;
	}
	
	public long executeBatchUpdate(String aSQL, List<Object[]> aParamsList) throws SQLException
	{
		Connection conn 		= null;
		PreparedStatement stmt	= null;
		
		long lAffectedRows 		= 0;
		try{
			conn 	= getConnection();
			
			stmt 	= conn.prepareStatement(aSQL);
			for(Object[] aParams : aParamsList)
			{
				stmt.clearParameters();
				stmt = setParams(stmt, aParams);
				lAffectedRows += stmt.executeUpdate();
			}
		}finally
		{
			closeQuietly(conn, stmt, null);
		}
		return lAffectedRows;
	}	

	public List<List<String>> executeQuery(String aSQL, List<Object> aParamList) throws SQLException
	{
		return executeQuery(aSQL, aParamList.toArray(new Object[aParamList.size()]));
	}

	public List<List<String>> executeQuery(String aSQL, Object[] aParams) throws SQLException
	{
		List<Object[]> listParams = new ArrayList<Object[]>();
		listParams.add(aParams);
		return executeBatchQuery(aSQL, listParams);
	}
	
	public long getExecuteQueryCount(String aSQL, Object[] aParams) throws SQLException
	{
		List<Object[]> listParams = new ArrayList<Object[]>();
		listParams.add(aParams);
		List<List<String>> listResult = executeBatchQuery(aSQL, listParams);
		return listResult.size()-1;
	}
	
	public List<List<String>> executeBatchQuery(String aSQL, List<Object[]> aParamsList) throws SQLException
	{
		List<List<String>> listData = new ArrayList<List<String>>();
		List<String> listCols = null;
		
		Connection conn 		= null;
		PreparedStatement stmt	= null;
		ResultSet rs 			= null;
		try{
			conn 	= getConnection();
			stmt 	= conn.prepareStatement(aSQL);
			
			for(Object[] oParams : aParamsList)
			{
				try {
					stmt 	= setParams(stmt, oParams);
					rs 		= stmt.executeQuery();
					
					//Column Name
					ResultSetMetaData meta = rs.getMetaData();
					int iTotalCols = meta.getColumnCount();
					listCols = new ArrayList<String>();
					for(int i=0; i<iTotalCols; i++)
					{
						listCols.add(meta.getColumnName(i+1));
					}
					listData.add(listCols);
		
					//Result
					while(rs.next())
					{
						listCols = new ArrayList<String>();
						for(int i=0; i<iTotalCols; i++)
						{
							listCols.add(rs.getString(i+1));
						}
						listData.add(listCols);
					}
				}
				finally
				{
					closeQuietly(null, stmt, rs);
				}
			}
		}finally
		{
			closeQuietly(conn, stmt, rs);
		}
		
		return listData;
	}
	
	public void closeQuietly(Connection aConn, PreparedStatement aStmt, ResultSet aResultSet ) throws SQLException
	{
		try{
			if(aResultSet!=null)
				aResultSet.close();
		}catch(Exception ex) { }
		//
		try{
			if(aStmt!=null)
				aStmt.close();
		}catch(Exception ex) { }
		//
		if(aConn!=null)
		{
			if(stackConns.size()<db_conn_pool_size)
			{
				stackConns.push(aConn);
			}
			else
			{
				aConn.close();
			}
		}
	}
}
