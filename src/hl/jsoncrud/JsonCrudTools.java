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

package hl.jsoncrud;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import hl.common.JdbcDBMgr;

public class JsonCrudTools {
	
	private static CRUDMgr crudMgr = null;
	
	public static String genPostgresTableMapping(String aJdbcConfigName, String[] aTableNames) throws SQLException
	{
		return genTableMapping(null, aJdbcConfigName, aTableNames);
	}
	
	public static String[] genTableNamesFromSchema(String aPropFileName, String aJdbcConfigName, String schemaName) throws SQLException
	{
		List<String> listTableName = new ArrayList<String>();
		
		if(schemaName==null || schemaName.trim().length()==0)
			schemaName = "public";
		
		JdbcDBMgr jdbc = initJdbcDBMgr(aPropFileName, aJdbcConfigName);
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = jdbc.getConnection();
			stmt = conn.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = ? order by table_name ");
			stmt.setString(1, schemaName);
			rs = stmt.executeQuery();
			while(rs.next())
			{
				listTableName.add(rs.getString(1));
			}
		}
		finally
		{
			if(jdbc!=null)
			{
				jdbc.closeQuietly(conn, stmt, rs);
			}
		}
		
		return (String[]) listTableName.toArray(new String[listTableName.size()]);
	}
	
	private static JdbcDBMgr initJdbcDBMgr(String aPropFileName, String aJdbcConfigName)
	{
		if(crudMgr==null)
		{
			crudMgr = new CRUDMgr(aPropFileName);
		}
		JdbcDBMgr jdbc = crudMgr.getJdbcMgr(aJdbcConfigName);
		return jdbc;
	}
	
	public static String genTableMappingBySchema(String aPropFileName, String aJdbcConfigName, String aSchemaName) throws SQLException
	{
		String[] sTableNames = genTableNamesFromSchema(aPropFileName, aJdbcConfigName, aSchemaName);
		
		System.out.println("Generating table mapping for ");
		for(int i=0; i<sTableNames.length; i++)
		{
			System.out.println("  "+(i+1)+". "+sTableNames[i]);
		}
		return genTableMapping(aPropFileName, aJdbcConfigName, sTableNames);
	}
	
	public static String genTableMapping(String aPropFileName, String aJdbcConfigName, String[] aTableNames) throws SQLException
	{
		if(aTableNames==null)
			return null;
		
		JdbcDBMgr jdbc = initJdbcDBMgr(aPropFileName, aJdbcConfigName);
		
		if(jdbc==null)
			throw new SQLException("Invalid jdbc config name : "+aJdbcConfigName);
		
		StringBuffer sb = new StringBuffer();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = jdbc.getConnection();
			
			for(String sTableName : aTableNames)
			{
				sb.setLength(0);
				sb.append("crud.").append(sTableName.toLowerCase()).append(".dbconfig=").append(aJdbcConfigName).append("\n");
				sb.append("crud.").append(sTableName.toLowerCase()).append(".tablename=").append(sTableName).append("\n");
				
				stmt = conn.prepareStatement("SELECT * FROM "+sTableName+" WHERE 1=2");
				rs = stmt.executeQuery();
				ResultSetMetaData meta = rs.getMetaData();
				
				for(int i=1; i<=meta.getColumnCount();i++)
				{
					String sColName = meta.getColumnName(i);
					
					boolean initialCap = false;
					String sJsonName = "";
					for(char ch : sColName.toCharArray())
					{
						if(ch=='_' || ch=='-')
						{
							initialCap = true;
							continue;
						}
						
						if(initialCap)
						{
							sJsonName += String.valueOf(ch).toUpperCase();
						}
						else
						{
							sJsonName += ch;
						}
						
						initialCap = false;
					}
					
					sb.append("crud.").append(sTableName.toLowerCase()).append(".jsonattr.").append(sJsonName).append(".colname=").append(sColName).append("\n");
					
				}
				
				System.out.println();
				System.out.println(sb.toString());
				rs.close();
				stmt.close();
			}
		}
		finally
		{
			if(jdbc!=null)
			{
				jdbc.closeQuietly(conn, stmt, rs);
			}
		}
		return null;
	}
	
	
	public static void main(String args[]) throws Exception
	{
		String sProFileName 	= null;
		String sJdbcConfigKey 	= null;
		String sSchemaName		= null;
		String[] sTableNames	= null;
		
		for(String arg : args)
		{
			if(arg.endsWith(".properties"))
			{
				sProFileName = arg;
			}
			else if(arg.startsWith("jdbc."))
			{
				sJdbcConfigKey = arg;
			}
			else if(arg.startsWith("schema="))
			{
				sSchemaName = arg.substring("schema=".length());
			}
			else if(arg.startsWith("tables="))
			{
				List<String> listTableName = new ArrayList<String>();
				StringTokenizer tk = new StringTokenizer(arg.substring("tables=".length()), ",");
				while(tk.hasMoreTokens())
				{
					listTableName.add(tk.nextToken());
				}
				sTableNames = listTableName.toArray(new String[listTableName.size()]);
			}
		}
		
		
		if(sJdbcConfigKey!=null)
		{
			if(sSchemaName!=null)
			{
				// [jsoncrud.properties] <jdbc.postgres> <schema=public>
				JsonCrudTools.genTableMappingBySchema(sProFileName, sJdbcConfigKey, sSchemaName);
			}
			else if(sTableNames!=null)
			{
				// [jsoncrud.properties] <jdbc.postgres> <tables=users,roles>
				JsonCrudTools.genTableMapping(sProFileName, sJdbcConfigKey, sTableNames);
			}
		}
	}
}
