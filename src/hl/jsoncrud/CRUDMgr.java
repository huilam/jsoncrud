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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import hl.common.JdbcDBMgr;

public class CRUDMgr {
	
	private final static String JSONFILTER_IN 					= "in";
	private final static String JSONFILTER_FROM 				= "from";
	private final static String JSONFILTER_TO 					= "to";
	private final static String JSONFILTER_STARTWITH			= "startwith";
	private final static String JSONFILTER_ENDWITH				= "endwith";
	private final static String JSONFILTER_CONTAIN				= "contain";
	private final static String JSONFILTER_CASE_INSENSITIVE		= "ci";
	private final static String JSONFILTER_NOT					= "not";
	
	private final static String SQL_IN_SEPARATOR				= ",";
	private final static String SQLLIKE_WILDCARD				= "%";
	private final static char[] SQLLIKE_RESERVED_CHARS			= new char[]{'%','_'};
	
	private final static String REGEX_JSONFILTER = "([a-zA-Z_-]+?)(?:\\.("+JSONFILTER_NOT+"))?"
			+"(?:\\.("+JSONFILTER_FROM+"|"+JSONFILTER_TO+"|"+JSONFILTER_IN+"|"
			+JSONFILTER_STARTWITH+"|"+JSONFILTER_ENDWITH+"|"+JSONFILTER_CONTAIN+"|"+JSONFILTER_NOT+"|"
			+JSONFILTER_CASE_INSENSITIVE+"))"
		+"(?:\\.("+JSONFILTER_CASE_INSENSITIVE+"|"+JSONFILTER_NOT+"))?"
		+"(?:\\.("+JSONFILTER_CASE_INSENSITIVE+"|"+JSONFILTER_NOT+"))?";
	
	private List<String> mapSQLmeta 							= new ArrayList<String>();

	private Map<String, JdbcDBMgr> mapDBMgr 					= null;
	private Map<String, Map<String, String>> mapJson2ColName 	= null;
	private Map<String, Map<String, String>> mapColName2Json 	= null;
	private Map<String, Map<String, String>> mapJson2Sql 		= null;
	private Map<String, Map<String,DBColMeta>> mapTableCols 	= null;
	
	private Pattern pattSQLjsonname		= null;
	private Pattern pattJsonColMapping 	= null;
	private Pattern pattJsonSQL 		= null;
	private Pattern pattJsonNameFilter			= null;
	private Pattern pattInsertSQLtableFields 	= null;
		
	private JsonCrudConfig jsoncrudConfig 		= null;
	private String config_prop_filename 		= null;
	
	private static Logger logger = Logger.getLogger(CRUDMgr.class.getName());
	
	public CRUDMgr()
	{
		config_prop_filename = null;
		init();
	}
	
	public JSONObject getVersionInfo()
	{
		JSONObject jsonVer = new JSONObject();
		jsonVer.put("framework", "jsoncrud");
		jsonVer.put("version", "0.6.0 beta");
		return jsonVer;
	}
	
	public CRUDMgr(String aPropFileName)
	{
		config_prop_filename = aPropFileName;
		if(aPropFileName!=null && aPropFileName.trim().length()>0)
		{
			try {
				jsoncrudConfig = new JsonCrudConfig(aPropFileName);
			} catch (IOException e) {
				throw new RuntimeException("Error loading "+aPropFileName, e);
			}
		}
		init();
	}
	
	public CRUDMgr(Properties aProp)
	{
		try {
			jsoncrudConfig = new JsonCrudConfig(aProp);
		} catch (IOException e) {
			throw new RuntimeException("Error loading Properties "+aProp.toString(), e);
		}
		init();
	}

	private void init()
	{
		mapDBMgr 			= new HashMap<String, JdbcDBMgr>();
		mapJson2ColName 	= new HashMap<String, Map<String, String>>();
		mapColName2Json 	= new HashMap<String, Map<String, String>>();
		mapJson2Sql 		= new HashMap<String, Map<String, String>>();
		mapTableCols 		= new HashMap<String, Map<String, DBColMeta>>();
		
		//
		pattJsonColMapping 	= Pattern.compile("jsonattr\\.([a-zA-Z_-]+?)\\.colname");
		pattJsonSQL 		= Pattern.compile("jsonattr\\.([a-zA-Z_-]+?)\\.sql");
		//
		pattSQLjsonname 			= Pattern.compile("\\{(.+?)\\}");
		pattInsertSQLtableFields 	= Pattern.compile("insert\\s+?into\\s+?([a-zA-Z_]+?)\\s+?\\((.+?)\\)");
		//
		pattJsonNameFilter 	= Pattern.compile(REGEX_JSONFILTER);
		
		try {
			reloadProps();
			
			if(jsoncrudConfig.getAllConfig().size()==0)
			{
				throw new IOException("Fail to load properties file - "+config_prop_filename);
			}
			
			
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		initPaginationConfig();
		initValidationErrCodeConfig();
	}

	private void initValidationErrCodeConfig()
	{
		Map<String, String> mapErrCodes = jsoncrudConfig.getConfig(JsonCrudConfig._DB_VALIDATION_ERRCODE_CONFIGKEY);
		if(mapErrCodes!=null && mapErrCodes.size()>0)
		{
			String sMetaKey = null;
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_EXCEED_SIZE);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_EXCEED_SIZE = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_TYPE);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_TYPE = sMetaKey;

			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_NOT_NULLABLE);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_NOT_NULLABLE = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_SYSTEM_FIELD);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_SYSTEM_FIELD = sMetaKey;
			
		}
		////////
		mapErrCodes = jsoncrudConfig.getConfig(JsonCrudConfig._JSONCRUD_FRAMEWORK_ERRCODE_CONFIGKEY);
		if(mapErrCodes!=null && mapErrCodes.size()>0)
		{
			String sMetaKey = null;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_JSONCRUDCFG);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_JSONCRUDCFG = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_SQLEXCEPTION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_SQLEXCEPTION = sMetaKey;		
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_PLUGINEXCEPTION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_PLUGINEXCEPTION = sMetaKey;		
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_FILTER);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_FILTER = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_SORTING);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_SORTING = sMetaKey;		
		}
	}

	private void initPaginationConfig()
	{
		Map<String, String> mapPagination = jsoncrudConfig.getConfig(JsonCrudConfig._PAGINATION_CONFIGKEY);
		
		if(mapPagination!=null && mapPagination.size()>0)
		{
			String sMetaKey = null;
			
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_META);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_META = sMetaKey;
			
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_RESULT);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_RESULT = sMetaKey;
			
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_TOTAL);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_TOTAL = sMetaKey;
	
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_FETCHSIZE);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_FETCHSIZE = sMetaKey;
			
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_START);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_START = sMetaKey;
					
			sMetaKey = mapPagination.get(JsonCrudConfig._LIST_SORTING);
			if(sMetaKey!=null)
				JsonCrudConfig._LIST_SORTING = sMetaKey;
		}		
		
	}
	
	public JSONObject checkJSONmapping(String aCrudKey, JSONObject aDataJson, Map<String, String> aCrudCfgMap) throws JsonCrudException
	{
		boolean isExceptionOnUnknownJsonAttr = false;
		String sExceptionOnUnknownAttr = aCrudCfgMap.get(JsonCrudConfig._PROP_KEY_EXCEPTION_ON_UNKNOWN_ATTR);
		if(sExceptionOnUnknownAttr!=null)
		{
			isExceptionOnUnknownJsonAttr = sExceptionOnUnknownAttr.trim().equalsIgnoreCase("true");
		}
		
		
		JSONObject jsonMappedObj = new JSONObject();
		
		Map<String, String> mapCrudJsonSql = mapJson2Sql.get(aCrudKey);
		Map<String, String> mapCrudJsonCol = mapJson2ColName.get(aCrudKey);
		Map<String, DBColMeta> mapCrudDBCols = mapTableCols.get(aCrudKey);
		
		for(String sJsonAttr : aDataJson.keySet())
		{
			boolean isInJsonCrudConfig 	= mapCrudJsonCol.get(sJsonAttr)!=null;
			boolean isInDBSchema 		= mapCrudDBCols.get(sJsonAttr)!=null;
			boolean isInJsonSQL			= mapCrudJsonSql.get(sJsonAttr)!=null;
			if(!isInJsonCrudConfig && !isInDBSchema && !isInJsonSQL)
			{
				if(isExceptionOnUnknownJsonAttr)
				{
					throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid input data ! - CrudKey:["+aCrudKey+"], Key:["+sJsonAttr+"]");
				}
				else
				{
					continue;
				}
			}

			jsonMappedObj.put(sJsonAttr, aDataJson.get(sJsonAttr));
		}
		return jsonMappedObj;
	}
	
	
	public JSONObject create(String aCrudKey, JSONObject aDataJson) throws JsonCrudException
	{
		Map<String, String> mapCrudCfg = jsoncrudConfig.getConfig(aCrudKey);
		if(mapCrudCfg==null || mapCrudCfg.size()==0)
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key ! - "+aCrudKey);
		
		JSONObject jsonData = checkJSONmapping(aCrudKey, aDataJson, mapCrudCfg);
		jsonData = castJson2DBVal(aCrudKey, jsonData);
		
		StringBuffer sbColName 	= new StringBuffer();
		StringBuffer sbParams 	= new StringBuffer();
		
		StringBuffer sbRollbackParentSQL = new StringBuffer();
		
		List<Object> listValues = new ArrayList<Object>();
		
		List<String> listUnmatchedJsonName = new ArrayList<String>();
		
		String sTableName 	= mapCrudCfg.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		//boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		
		Map<String, String> mapCrudJsonCol = mapJson2ColName.get(aCrudKey);
		for(String sJsonName : jsonData.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			if(sColName!=null)
			{
				Object o = jsonData.get(sJsonName);
				if(o!=null  && o!=JSONObject.NULL)
				{
					listValues.add(o);
					//
					if(sbColName.length()>0)
					{
						sbColName.append(",");
						sbParams.append(",");
					}
					sbColName.append(sColName);
					sbParams.append("?");
					//
	
					//
					sbRollbackParentSQL.append(" AND ").append(sColName).append(" = ? ");
					//
				}
				
			}
			else
			{
				listUnmatchedJsonName.add(sJsonName);
			}
		}
		
		String sSQL 		= "INSERT INTO "+sTableName+"("+sbColName.toString()+") values ("+sbParams.toString()+")";
		
		String sJdbcName 	= mapCrudCfg.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		JSONArray jArrCreated = null; 
		
		try {
			jArrCreated = dbmgr.executeUpdate(sSQL, listValues);
		}
		catch(Throwable ex)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "sql:"+sSQL+", params:"+listParamsToString(listValues), ex);
		}

		
		if(jArrCreated.length()>0)
		{
			JSONObject jsonCreated = jArrCreated.getJSONObject(0);
			jsonCreated = convertCol2Json(aCrudKey, jsonCreated);
			for(String sAttrName : jsonCreated.keySet())
			{
				jsonData.put(sAttrName, jsonCreated.get(sAttrName));
			}
			
			//child create
			if(listUnmatchedJsonName.size()>0)
			{
				JSONArray jsonArrReturn = retrieve(aCrudKey, jsonData);
				
				for(int i=0 ; i<jsonArrReturn.length(); i++  )
				{
					JSONObject jsonReturn = jsonArrReturn.getJSONObject(i);
							
					//merging json obj
					for(String sDataJsonKey : jsonData.keySet())
					{
						jsonReturn.put(sDataJsonKey, jsonData.get(sDataJsonKey));
					}
					
					for(String sJsonName2 : listUnmatchedJsonName)
					{
						List<Object[]> listParams2 	= getSubQueryParams(mapCrudCfg, jsonReturn, sJsonName2);
						String sObjInsertSQL 		= mapCrudCfg.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_CHILD_INSERTSQL);
						
						long lupdatedRow 	= 0;
						
						try {
							lupdatedRow 	= updateChildObject(dbmgr, sObjInsertSQL, listParams2);
						}
						catch(Throwable ex)
						{
							try {
								//rollback parent
								sbRollbackParentSQL.insert(0, "DELETE FROM "+sTableName+" WHERE 1=1 ");

								JSONArray jArrRollbackRows = dbmgr.executeUpdate(sbRollbackParentSQL.toString(), listValues);
								if(jArrCreated.length() != jArrRollbackRows.length())
								{
									throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "Record fail to Rollback!");
								}
							}
							catch(Throwable ex2)
							{
								throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "[Rollback Failed], parent:[sql:"+sbRollbackParentSQL.toString()+",params:"+listParamsToString(listValues)+"], child:[sql:"+sObjInsertSQL+",params:"+listParamsToString(listParams2)+"]", ex);
							}
							
							throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "[Rollback Success] : child : sql:"+sObjInsertSQL+", params:"+listParamsToString(listParams2), ex);
						}
							
					}
				}
			}			
			
			JSONArray jsonArray = retrieve(aCrudKey, jsonData);
			if(jsonArray==null || jsonArray.length()==0)
				return null;
			else
				return (JSONObject) jsonArray.get(0);
		}
		else
		{
			return null;
		}
	}
	
	public JSONObject retrieveFirst(String aCrudKey, JSONObject aWhereJson) throws JsonCrudException
	{
		JSONArray jsonArr = retrieve(aCrudKey, aWhereJson);
		if(jsonArr!=null)
		{
			if(jsonArr.length()>0)
				return (JSONObject) jsonArr.get(0);
			else
				return new JSONObject(); //empty result
		}
		return null;
	}
	
	public JSONArray retrieve(String aCrudKey, JSONObject aWhereJson) throws JsonCrudException
	{
		JSONObject json = retrieve(aCrudKey, aWhereJson, 0, 0, null, null);
		if(json==null)
		{
			return new JSONArray();
		}
		return (JSONArray) json.get(JsonCrudConfig._LIST_RESULT);
	}
	
	public JSONArray retrieve(String aCrudKey, JSONObject aWhereJson, String[] aSorting, String[] aReturns) throws JsonCrudException
	{
		JSONObject json = retrieve(aCrudKey, aWhereJson, 0, 0, aSorting, aReturns);
		if(json==null)
		{
			return new JSONArray();
		}
		return (JSONArray) json.get(JsonCrudConfig._LIST_RESULT);
	}
	
	public JSONObject retrieveBySQL(String aCrudKey, String aSQL, Object[] aObjParams,
			long aStartFrom, long aFetchSize) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, aSQL, aObjParams, aStartFrom, aFetchSize, null);
	}
	
	public long executeSQL(String aCrudKey, String aSQL, Object[] aObjParams) throws JsonCrudException
	{
		long lAffectRows = 0;
		
		Map<String, String> map 		= jsoncrudConfig.getConfig(aCrudKey);
		if(map==null)
			return 0;
		
		String sSQL 		= aSQL;
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		Connection conn 		= null;
		PreparedStatement stmt	= null;
		
		try{
			
			conn = dbmgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			stmt = JdbcDBMgr.setParams(stmt, aObjParams);
			lAffectRows  = stmt.executeUpdate();
		}
		catch(SQLException sqlEx)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams), sqlEx);
		}
		finally
		{
			try {
				if(dbmgr!=null)
					dbmgr.closeQuietly(conn, stmt, null);
			} catch (SQLException e) {
				throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, e);
			}
		}
		
		return lAffectRows;
	}
	
	private long getTotalSQLCount(String aCrudKey, String aSQL, Object[] aObjParams) throws JsonCrudException
	{
		Map<String, String> map 		= jsoncrudConfig.getConfig(aCrudKey);
		if(map==null)
			return 0;
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		long lTotalCount = 0;
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		try {
			conn = dbmgr.getConnection();
			stmt = conn.prepareStatement(aSQL);
			stmt = JdbcDBMgr.setParams(stmt, aObjParams);
			rs = stmt.executeQuery();
			if(rs.next())
			{
				lTotalCount = rs.getLong(1);
			}
		} 
		catch (SQLException sqlEx) 
		{
			System.err.println(aSQL);
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, 
					"crudKey:"+aCrudKey+", sql:"+aSQL+", params:"+listParamsToString(aObjParams), sqlEx);	
		}
		finally
		{
			try {
				dbmgr.closeQuietly(conn, stmt, rs);
			} catch (SQLException e) {
			}
		}
		return lTotalCount;
	}
	
	private JSONObject retrieveBySQL(String aCrudKey, String aSQL, Object[] aObjParams,
			long aStartFrom, long aFetchSize, String[] aReturns) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, aSQL, aObjParams, aStartFrom, aFetchSize, aReturns, 0);
	}
	
	private JSONObject retrieveBySQL(String aCrudKey, String aSQL, Object[] aObjParams,
			long aStartFrom, long aFetchSize, String[] aReturns, 
			long aTotalRecordCount) throws JsonCrudException
	{
		JSONObject jsonReturn 			= null;
		Map<String, String> map 		= jsoncrudConfig.getConfig(aCrudKey);
		if(map==null)
			return null;
		
		Map<String, String> mapCrudSql 	= mapJson2Sql.get(aCrudKey);
		
		String sSQL 		= aSQL;
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		List<String> listReturnsAttrName = new ArrayList<String>();
		if(aReturns!=null)
		{
			for(String sAttrName : aReturns)
			{
				listReturnsAttrName.add(sAttrName);
			}
		}
		
		int iFetchSize = dbmgr.getDBFetchSize();
		boolean isFilterByReturns = listReturnsAttrName.size()>0;
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		
		if(aStartFrom<=0)
			aStartFrom = 1;
		
		
		JSONArray jsonArr = new JSONArray();
		try{
			
			conn = dbmgr.getConnection();
			
			stmt = conn.prepareStatement(sSQL);
			stmt = JdbcDBMgr.setParams(stmt, aObjParams);
			if(iFetchSize>0)
			{
				conn.setAutoCommit(false);
				stmt.setFetchSize(iFetchSize);
			}
			
			rs   = stmt.executeQuery();
			
			ResultSetMetaData meta 	= rs.getMetaData();
			long lTotalResult 		= 0;
			while(rs.next())
			{	
				lTotalResult++;
				
				if(lTotalResult < aStartFrom)
					continue;
				
				JSONObject jsonOnbj = new JSONObject();
				
				for(int i=0; i<meta.getColumnCount(); i++)
				{
					// need to have full result so that subquery can be execute
					String sColName = meta.getColumnLabel(i+1);					
					Object oObj = rs.getObject(sColName);
					if(oObj==null)
						oObj = JSONObject.NULL;
					jsonOnbj.put(sColName, oObj);
	
				}
				
				jsonOnbj = convertCol2Json(aCrudKey, jsonOnbj);
				
				if(mapCrudSql.size()>0)
				{
					for(String sJsonName : mapCrudSql.keySet())
					{
						
						if(isFilterByReturns)
						{
							if(!listReturnsAttrName.contains(sJsonName))
								//skip
								continue;
						}
						
						if(!jsonOnbj.has(sJsonName))
						{
							List<Object> listParams2 = new ArrayList<Object>();
							String sSQL2 = mapCrudSql.get(sJsonName);
							Matcher m = pattSQLjsonname.matcher(sSQL2);
							while(m.find())
							{
								String sBracketJsonName = m.group(1);
								if(jsonOnbj.has(sBracketJsonName))
								{
									listParams2.add(jsonOnbj.get(sBracketJsonName));
								}
							}
							sSQL2 = sSQL2.replaceAll("\\{.+?\\}", "?");
							
							if(sSQL2.indexOf("?")>-1)
							{
								if(listParams2.size()==0)
								{
									break;
									//throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, 
									//		"Insufficient sql paramters - sql:"+mapCrudSql.get(sJsonName)+", params:"+listParamsToString(listParams2));
								}
								
							}
							
							String sSQL2FetchLimit = map.get("jsonattr."+sJsonName+".sql.fetch.limit");
							long lFetchLimit = 0;
							if(sSQL2FetchLimit!=null)
							{
								try {
									lFetchLimit = Integer.parseInt(sSQL2FetchLimit);
								}catch(NumberFormatException ex)
								{
									ex.printStackTrace();
									lFetchLimit = 0;
								}
							}
							
							JSONArray jsonArrayChild = retrieveChild(dbmgr, conn, sSQL2, listParams2, 1, lFetchLimit);
							
							if(jsonArrayChild!=null)
							{
								String sPropKeyMapping = "jsonattr."+sJsonName+"."+JsonCrudConfig._PROP_KEY_CHILD_MAPPING;
								String sChildMapping = map.get(sPropKeyMapping);
								
								///
								if(sChildMapping!=null)
								{
									sChildMapping = sChildMapping.trim();
									
									if(sChildMapping.startsWith("\"") && sChildMapping.endsWith("\""))
									{
										String sData = "";
										if(jsonArrayChild.length()>0)
										{
											Object oData = jsonArrayChild.get(0);
											if(oData!=null)
											{
												String sAttrkey = sChildMapping.substring(1, sChildMapping.length()-1);
												
												if(oData instanceof JSONObject)
												{
													JSONObject jsonData = ((JSONObject)oData);
													if(jsonData.has(sAttrkey))
													{
														sData = String.valueOf(jsonData.get(sAttrkey));
													}
												}
												else
												{
													sData = String.valueOf(oData);
												}
												
											}
										}
										jsonOnbj.put(sJsonName, sData);
									}
									else if(sChildMapping.startsWith("[") && sChildMapping.endsWith("]"))
									{
										JSONArray jArrMappingData = new JSONArray();
										JSONArray jArrMapping = new JSONArray(sChildMapping);
										for(int i=0; i<jsonArrayChild.length(); i++)
										{		
											JSONObject jsonData = jsonArrayChild.getJSONObject(i);
											for(int j=0; j<jArrMapping.length(); j++)
											{
												String sKey = jArrMapping.getString(j);
												if(jsonData.has(sKey))
												{
													Object oMappingData = jsonData.get(sKey);
													jArrMappingData.put(oMappingData);
												}
												else
												{
													throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, 
															"Invalid Child Mapping : "+sPropKeyMapping+"="+sChildMapping);
												}
											}
										}
										jsonOnbj.put(sJsonName, jArrMappingData);
									}
									else if(sChildMapping.startsWith("{") && sChildMapping.endsWith("}"))
									{
										JSONObject jsonMappingData = null;
										JSONObject jsonChildMapping = new JSONObject(sChildMapping);
										int iKeys = jsonChildMapping.keySet().size();
										if(iKeys>0)
										{
											jsonMappingData = new JSONObject();
											for(int i=0; i<jsonArrayChild.length(); i++)
											{		
												JSONObject jsonData = jsonArrayChild.getJSONObject(i);
												for(String sKey : jsonChildMapping.keySet())
												{
													String sMapKey = jsonData.getString(sKey);
													Object oMapVal = jsonData.get(jsonChildMapping.getString(sKey));
													
													jsonMappingData.put(sMapKey, oMapVal);
												}
											}
										}
										
										if(jsonMappingData!=null)
										{
											jsonOnbj.put(sJsonName, jsonMappingData);
										}
										else 
											throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, 
													"Invalid Child Mapping : "+sPropKeyMapping+"="+sChildMapping);
									}
									
								}
								else
								{
									jsonOnbj.put(sJsonName, jsonArrayChild);			
								}
							}
						}
					}
				}
				
				
				if(isFilterByReturns)
				{
					JSONObject jsonObjReturn = new JSONObject();
					for(Object oAttrKey : jsonOnbj.keySet())
					{
						String sAttKey = oAttrKey.toString();
						if(listReturnsAttrName.contains(sAttKey))
						{
							jsonObjReturn.put(sAttKey, jsonOnbj.get(sAttKey));
						}
					}
					jsonOnbj = jsonObjReturn;
					
				}
				
				jsonArr.put(jsonOnbj);
				
				if(aFetchSize>0 && jsonArr.length()>=aFetchSize)
				{
					break;
				}
			}
			
			long lTotalCount = lTotalResult;
			
			
			if(aTotalRecordCount<=0)
			{
				while(rs.next())
				{
					lTotalCount++;	
				}
			}
			else
			{
				lTotalCount = aTotalRecordCount;
			}
			
			if(jsonArr!=null)
			{
				jsonReturn = new JSONObject();
				jsonReturn.put(JsonCrudConfig._LIST_RESULT, jsonArr);
				//
				JSONObject jsonMeta = new JSONObject();
				jsonMeta.put(JsonCrudConfig._LIST_TOTAL, lTotalCount);
				jsonMeta.put(JsonCrudConfig._LIST_START, aStartFrom);
				//
				if(aFetchSize>0)
					jsonMeta.put(JsonCrudConfig._LIST_FETCHSIZE, aFetchSize);
				//
				jsonReturn.put(JsonCrudConfig._LIST_META, jsonMeta);
			}
			
		}
		catch(SQLException sqlEx)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams), sqlEx);
		}
		finally
		{
			if(conn!=null)
			{
				//
				if(iFetchSize>0)
				{
					try {
						conn.commit();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
					//
					try {
						conn.setAutoCommit(true);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			
			try {
				if(dbmgr!=null)
					dbmgr.closeQuietly(conn, stmt, rs);
			} catch (SQLException e) {
				throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, e);
			}
		}
		return jsonReturn;		
	}
	
	private JSONArray retrieveChild(JdbcDBMgr aJdbcMgr, Connection aConnection, String aSQL, List<Object> aObjParamList, long iFetchStart, long iFetchSize) throws SQLException
	{
		PreparedStatement stmt2	= null;
		ResultSet rs2 = null;
		int iTotalCols = 0;
		
		JSONArray jsonArr2 	= null;

		try{
			stmt2 = aConnection.prepareStatement(aSQL);
			stmt2 = JdbcDBMgr.setParams(stmt2, aObjParamList);
			rs2   = stmt2.executeQuery();
			
			///
			ResultSetMetaData meta = rs2.getMetaData();
			iTotalCols = meta.getColumnCount();
			if(iTotalCols>0)
				jsonArr2 = new JSONArray();
			
			long lTotalCnt = 0;
			
			if(iFetchStart<=0)
				iFetchStart = 1;
			
			if(iFetchSize<0)
				iFetchSize = 0;

			while(rs2.next())
			{
				lTotalCnt++;
				
				if(lTotalCnt<iFetchStart)
					continue;
				
				JSONObject json2 	= new JSONObject();
				for(int i=1; i<=iTotalCols; i++)
				{
					
					String sColName = meta.getColumnLabel(i);
					Object o = rs2.getObject(i);
					if(o==null)
						o = JSONObject.NULL;
					json2.put(sColName, o);
				}
				jsonArr2.put(json2);
				
				if(iFetchSize>0 && lTotalCnt>=iFetchSize)
					continue;
			}
			
		}catch(SQLException sqlEx)
		{
			throw new SQLException("sql:"+aSQL+", params:"+listParamsToString(aObjParamList), sqlEx);
		}
		finally{
			aJdbcMgr.closeQuietly(null, stmt2, rs2);
		}
		return jsonArr2;
	}
	
	public JSONObject retrieve(String aCrudKey, JSONObject aWhereJson, 
			long aStartFrom, long aFetchSize, String[] aSorting, String[] aReturns) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, null, aWhereJson, aStartFrom, aFetchSize, aSorting, aReturns);
	}
	
	public JSONObject retrieveBySQL(String aCrudKey, 
			String aTableViewSQL,
			JSONObject aWhereJson, 
			long aStartFrom, long aFetchSize, 
			String[] aSorting, String[] aReturns) throws JsonCrudException
	{
		
		try {
			cacheTableMetaDataBySQL(aCrudKey, aTableViewSQL);
		} catch (SQLException e) {
			// Silent, as this is just a pre-process
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		
		JSONObject jsonWhere = castJson2DBVal(aCrudKey, aWhereJson);
		if(jsonWhere==null)
			jsonWhere = new JSONObject();
		
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key ! - "+aCrudKey);
		
		List<Object> listValues 			= new ArrayList<Object>();		
		Map<String, String> mapCrudJsonCol 	= mapJson2ColName.get(aCrudKey);

		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);

		if(sTableName==null || sTableName.trim().length()==0)
		{
			return null;
		}
		
		// WHERE
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sOrgJsonName : jsonWhere.keySet())
		{
			boolean isCaseInSensitive 	= false;
			boolean isNotCondition		= false;
			String sOperator 	= " = ";
			String sJsonName 	= sOrgJsonName;
			Object oJsonValue 	= jsonWhere.get(sOrgJsonName);
			Map<String, String> mapSQLEscape = new HashMap<String, String>();
			
			if(sJsonName.indexOf(".")>-1)
			{
				Matcher m = pattJsonNameFilter.matcher(sJsonName);
				if(m.find())
				{
					sJsonName = m.group(1);
					String sJsonNOT 		= m.group(2);
					String sJsonOperator 	= m.group(3);
					String sJsonCIorNOT_1 	= m.group(4);
					String sJsonCIorNOT_2 	= m.group(5);

					if(JSONFILTER_CASE_INSENSITIVE.equalsIgnoreCase(sJsonCIorNOT_1) || JSONFILTER_CASE_INSENSITIVE.equalsIgnoreCase(sJsonCIorNOT_2) 
							|| JSONFILTER_CASE_INSENSITIVE.equals(sJsonOperator))
					{
						isCaseInSensitive = true;
					}
					
					if(JSONFILTER_NOT.equalsIgnoreCase(sJsonNOT)
							|| JSONFILTER_NOT.equalsIgnoreCase(sJsonCIorNOT_1) || JSONFILTER_NOT.equalsIgnoreCase(sJsonCIorNOT_2) 
							|| JSONFILTER_NOT.equals(sJsonOperator))
					{
						isNotCondition = true;
					}
					
					if(JSONFILTER_FROM.equals(sJsonOperator))
					{
						sOperator = " >= ";
					}
					else if(JSONFILTER_TO.equals(sJsonOperator))
					{
						sOperator = " <= ";
					}
					else if(JSONFILTER_TO.equals(sJsonOperator))
					{
						sOperator = " <= ";
					}
					else if(JSONFILTER_IN.equals(sJsonOperator))
					{
						sOperator = " IN ";
					}
					else if(oJsonValue!=null && oJsonValue instanceof String)
					{
						String sJsonValue = String.valueOf(oJsonValue);
						
						if(isRequireSQLEscape(sJsonValue))
						{
							String sEscapeChar = getSQLEscapeChar(sJsonValue);
							mapSQLEscape.put(sOrgJsonName, " ESCAPE '"+sEscapeChar+"' ");
							
							for(char c : SQLLIKE_RESERVED_CHARS)
							{
								sJsonValue = sJsonValue.replaceAll(String.valueOf(c), sEscapeChar+c);
							}
						}
						
						if(JSONFILTER_STARTWITH.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = sJsonValue+SQLLIKE_WILDCARD;
						}
						else if (JSONFILTER_ENDWITH.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = SQLLIKE_WILDCARD+sJsonValue;
						}
						else if (JSONFILTER_CONTAIN.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = SQLLIKE_WILDCARD+sJsonValue+SQLLIKE_WILDCARD;
						}
					}
				}
			}
			
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			if(sColName!=null)
			{
				sbWhere.append(" AND ");
				
				if(oJsonValue==null || oJsonValue==JSONObject.NULL)
				{
					sbWhere.append(sColName).append(" IS NULL ");
					continue;
				}
				
				if(isNotCondition)
				{
					sbWhere.append(" NOT (");
				}
				
				StringBuffer sbSQLparam = new StringBuffer();
				sbSQLparam.append("?");
				
				if(sOperator.trim().equalsIgnoreCase("IN"))
				{
					//multi-value
					sbSQLparam.setLength(0);
					String sStrValues = oJsonValue.toString();
					StringTokenizer tk = new StringTokenizer(sStrValues, SQL_IN_SEPARATOR);
					while(tk.hasMoreTokens())
					{
						String sVal = tk.nextToken();
						oJsonValue = castJson2DBVal(aCrudKey, sJsonName, sVal.trim());
						
						if(sbSQLparam.length()>0)
							sbSQLparam.append(", ");
						sbSQLparam.append("?");
						listValues.add(oJsonValue);
					}
					
				}
				else
				{
					oJsonValue = castJson2DBVal(aCrudKey, sJsonName, oJsonValue);
					listValues.add(oJsonValue);
				}
				
				
				if(isCaseInSensitive && (oJsonValue instanceof String))
				{
					sbWhere.append(" UPPER(").append(sColName).append(") ").append(sOperator).append(" UPPER(").append(sbSQLparam.toString()).append(")");
				}
				else 
				{
					sbWhere.append(sColName).append(sOperator).append(" (").append(sbSQLparam.toString()).append(")");
				}
				
				
				if(sOperator.equalsIgnoreCase(" like "))
				{
					String sEscapeSQL = mapSQLEscape.get(sOrgJsonName);
					if(sEscapeSQL!=null)
						sbWhere.append(sEscapeSQL);
				}
				
				if(isNotCondition)
				{
					sbWhere.append(" )");
				}
				// 

			}
			else 
			{
				Map<String, String> mapJsonSql = mapJson2Sql.get(aCrudKey);
				if(mapJsonSql==null || mapJsonSql.get(sJsonName)==null)
					throw new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_FILTER, "Invalid filter - "+aCrudKey+" : "+sJsonName);
			}
		}
		
		List<String> listSelectFields = new ArrayList<String>();
		
		StringBuffer sbOrderBy = new StringBuffer();
		if(aSorting!=null && aSorting.length>0)
		{
			for(String sOrderBy : aSorting)
			{
				String sOrderSeqKeyword = "";
				int iOrderSeq = sOrderBy.indexOf('.');
				if(iOrderSeq>-1)
				{
					sOrderSeqKeyword = sOrderBy.substring(iOrderSeq+1);
				}
				else if(iOrderSeq==-1)
				{
					iOrderSeq = sOrderBy.length();
				}
					
				String sJsonAttr = sOrderBy.substring(0, iOrderSeq);
				String sOrderColName = mapCrudJsonCol.get(sJsonAttr);
				
				if(sOrderColName!=null)
				{
					if(sbOrderBy.length()>0)
					{
						sbOrderBy.append(",");
					}
					sbOrderBy.append(sOrderColName);
					if(sOrderSeqKeyword.length()>0)
						sbOrderBy.append(" ").append(sOrderSeqKeyword);
					
					
					if(!listSelectFields.contains(sOrderColName.toUpperCase()))
					{
						listSelectFields.add(sOrderColName.toUpperCase());
					}

				}
				else
				{
					throw new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_SORTING, "Invalid sorting - "+aCrudKey+" : "+sJsonAttr);
				}
			}
		}
		
		if(aTableViewSQL!=null && aTableViewSQL.length()>0)
		{
			sTableName = "("+aTableViewSQL+") AS TBL ";
		}
		
		if(aReturns!=null)
		{
			for(String sReturn : aReturns)
			{
				String sDBColName = mapCrudJsonCol.get(sReturn);
				if(sDBColName==null)
					sDBColName = sReturn;
				
				if(!listSelectFields.contains(sDBColName.toUpperCase()))
				{
					listSelectFields.add(sDBColName.toUpperCase());
				}
			}
		}
		
		StringBuffer sbSelectFields = new StringBuffer();
		if(aTableViewSQL==null || aTableViewSQL.length()==0)
		{
			//limit result when return fields are specified
			if(listSelectFields.size()>0 
					&& aReturns!=null && aReturns.length>0)
			{
				for(String aField : listSelectFields)
				{
					if(sbSelectFields.length()>0)
						sbSelectFields.append(", ");
					sbSelectFields.append(aField);
				}
			}
		}
		
		if(sbSelectFields.length()==0)
		{
			sbSelectFields.append("*");
		}
		
		Object[] objParams = listValues.toArray(new Object[listValues.size()]);
		StringBuffer sbSQL = new StringBuffer();
		sbSQL.append("SELECT COUNT(*) FROM ").append(sTableName).append(" WHERE 1=1 ").append(sbWhere.toString());
		
		long lTotalRecordCount = getTotalSQLCount(aCrudKey, sbSQL.toString(), objParams);
		
		sbSQL.setLength(0);
		sbSQL.append(" SELECT ").append(sbSelectFields.toString()).append(" FROM ").append(sTableName);
		sbSQL.append(" WHERE 1=1 ").append(sbWhere.toString());
		if(sbOrderBy.length()>0)
		{
			sbSQL.append(" ORDER BY ").append(sbOrderBy.toString());
		}
		
		JSONObject jsonReturn 	= retrieveBySQL(
				aCrudKey, sbSQL.toString(), 
				objParams, 
				aStartFrom, aFetchSize, aReturns, lTotalRecordCount);
		
		if(jsonReturn!=null && jsonReturn.has(JsonCrudConfig._LIST_META))
		{
			if(aSorting!=null)
			{
				JSONObject jsonMeta 	= jsonReturn.getJSONObject(JsonCrudConfig._LIST_META);
				StringBuffer sbOrderBys = new StringBuffer();
				for(String sOrderBy : aSorting)
				{
					if(sbOrderBys.length()>0)
						sbOrderBys.append(",");
					sbOrderBys.append(sOrderBy);
				}
				if(sbOrderBys.length()>0)
					jsonMeta.put(JsonCrudConfig._LIST_SORTING, sbOrderBys.toString());
				
				jsonReturn.put(JsonCrudConfig._LIST_META, jsonMeta);
			}
			//
		}
		
		return jsonReturn;
//
	}	
	
	private String getSQLEscapeChar(String aSqlStrValue)
	{
		for(char ch : JsonCrudConfig.SQLLIKE_ESCAPE_CHARS)
		{
			if(aSqlStrValue.indexOf(ch)==-1)
			{
				return String.valueOf(ch);
			}
		}
		return null;
	}
	
	private boolean isRequireSQLEscape(String aSqlStrValue) 
	{
		for(char cReservedChar : SQLLIKE_RESERVED_CHARS)
		{
			if(aSqlStrValue.indexOf(cReservedChar)>-1)
			{
				return true;
			}
		}
		return false;
	}
	
	public JSONArray update(String aCrudKey, JSONObject aDataJson, JSONObject aWhereJson) throws JsonCrudException
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map.size()==0)
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key ! - "+aCrudKey);

		JSONObject jsonData = checkJSONmapping(aCrudKey, aDataJson, map);
		jsonData = castJson2DBVal(aCrudKey, jsonData);
		JSONObject jsonWhere 	= castJson2DBVal(aCrudKey, aWhereJson);
		
		List<Object> listValues 			= new ArrayList<Object>();
		Map<String, String> mapCrudJsonCol 	= mapJson2ColName.get(aCrudKey);

		List<String> listUnmatchedJsonName	= new ArrayList<String>();
		//SET
		StringBuffer sbSets 	= new StringBuffer();
		for(String sJsonName : jsonData.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			if(sColName!=null)
			{
				//
				if(sbSets.length()>0)
					sbSets.append(",");
				sbSets.append(sColName).append(" = ? ");
				//
				listValues.add(jsonData.get(sJsonName));
			}
			else
			{
				listUnmatchedJsonName.add(sJsonName);
			}
		}
		// WHERE
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sJsonName : jsonWhere.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			if(sColName==null)
			{
				throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Missing Json to dbcol mapping ("+sJsonName+":"+jsonWhere.get(sJsonName)+") ! - "+aCrudKey);
			}
			
			sbWhere.append(" AND ").append(sColName).append(" = ? ");
			//
			listValues.add(jsonWhere.get(sJsonName));
		}
		
		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		//boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		String sSQL			= "UPDATE "+sTableName+" SET "+sbSets.toString()+" WHERE 1=1 "+sbWhere.toString();
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		JSONArray jArrUpdated = new JSONArray();
		long lAffectedRow2 	= 0;
		
		try{
			
			if(sbSets.length()>0)
			{
				jArrUpdated = dbmgr.executeUpdate(sSQL, listValues);
			}
			
			if(jArrUpdated.length()>0 || sbSets.length()==0)
			{
				//child update
				
				JSONArray jsonArrReturn = retrieve(aCrudKey, jsonWhere);
				
				for(int i=0 ; i<jsonArrReturn.length(); i++  )
				{
					JSONObject jsonReturn = jsonArrReturn.getJSONObject(i);
							
					//merging json obj
					for(String sDataJsonKey : jsonData.keySet())
					{
						jsonReturn.put(sDataJsonKey, jsonData.get(sDataJsonKey));
					}
					
					for(String sJsonName2 : listUnmatchedJsonName)
					{
						List<Object[]> listParams2 	= getSubQueryParams(map, jsonReturn, sJsonName2);
						String sObjInsertSQL 		= map.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_CHILD_INSERTSQL);
						
						lAffectedRow2 += updateChildObject(dbmgr, sObjInsertSQL, listParams2);
					}
				}
			}
		}
		catch(SQLException sqlEx) 
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "sql:"+sSQL+", params:"+listParamsToString(listValues), sqlEx);
		}
		
		if(jArrUpdated.length()>0 || lAffectedRow2>0)
		{
			JSONArray jsonArray = retrieve(aCrudKey, jsonWhere);
			return jsonArray;
		}
		else
		{
			return null;
		}
	}
	
	public JSONArray delete(String aCrudKey, JSONObject aWhereJson) throws JsonCrudException
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key ! - "+aCrudKey);
		
		JSONObject jsonWhere = castJson2DBVal(aCrudKey, aWhereJson);
		
		List<Object> listValues 		= new ArrayList<Object>();
		Map<String, String> mapCrudJsonCol = mapJson2ColName.get(aCrudKey);
		
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sJsonName : jsonWhere.keySet())
		{
			Object o = jsonWhere.get(sJsonName);
			String sColName = mapCrudJsonCol.get(sJsonName);
			
			if(o==null || o==JSONObject.NULL)
			{
				sbWhere.append(" AND ").append(sColName).append(" IS NULL ");
			}
			else
			{
				sbWhere.append(" AND ").append(sColName).append(" = ? ");
				listValues.add(o);
			}
			//
		}
		
		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		//boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		String sSQL 		= "DELETE FROM "+sTableName+" WHERE 1=1 "+sbWhere.toString();
		
		String sJdbcName = map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr = mapDBMgr.get(sJdbcName);
		
		JSONArray jsonArray = null;
		
		jsonArray = retrieve(aCrudKey, jsonWhere);
		
		if(jsonArray.length()>0)
		{
			JSONArray jArrAffectedRow = new JSONArray();
			try {
				jArrAffectedRow = dbmgr.executeUpdate(sSQL, listValues);
			}
			catch(SQLException sqlEx)
			{
				throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "sql:"+sSQL+", params:"+listParamsToString(listValues), sqlEx);
			}
			
			if(jArrAffectedRow.length()>0)
			{
				return jsonArray;
			}
		}
		return null;
	}	
	
	private void clearAll()
	{
		mapDBMgr.clear();
		mapJson2ColName.clear();
		mapColName2Json.clear();
		mapJson2Sql.clear();
		mapTableCols.clear();
	}

	public JsonCrudConfig getJsonCrudConfig() 
	{
		return jsoncrudConfig;
	}
	
	public Map<String, String> getAllConfig() 
	{
		return jsoncrudConfig.getAllConfig();
	}
	
	public Map<String, String> getCrudConfigs(String aConfigKey) 
	{
		if(!aConfigKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD))
			aConfigKey = JsonCrudConfig._PROP_KEY_CRUD + "." + aConfigKey;
		
		Map<String, String> mapCrudCfg = jsoncrudConfig.getConfig(aConfigKey);
		
		if(mapCrudCfg!=null && mapCrudCfg.size()>0)
		{
			JdbcDBMgr jdbcMgr = mapDBMgr.get(aConfigKey);
			if(jdbcMgr!=null)
			{
				Map<String , String> mapJdbcCfg = jdbcMgr.getReferenceConfig();
				if(mapJdbcCfg!=null)
				{
					mapCrudCfg.putAll(mapJdbcCfg);
				}
			}
		}
		return mapCrudCfg;
	}
	
	private JdbcDBMgr initNRegJdbcDBMgr(String aJdbcConfigKey, Map<String, String> mapJdbcConfig) throws SQLException
	{
		JdbcDBMgr dbmgr = mapDBMgr.get(aJdbcConfigKey);
		
		
		if(dbmgr==null)
		{
			String sJdbcClassName 	= mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME);
			String sJdbcUrl 		= mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_URL);
			String sJdbcUid 		= mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_UID);
			String sJdbcPwd 		= mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_PWD);

			if(sJdbcClassName!=null && sJdbcUrl!=null)
			{
				try {
					dbmgr = new JdbcDBMgr(sJdbcClassName, sJdbcUrl, sJdbcUid, sJdbcPwd);
					
					long lJdbcFetchSize 		= strToLong(mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_FETCHSIZE),0);
					long lJdbcMaxConn 			= strToLong(mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_MAXCONNS),0);
					long lJdbcWaitIntervalMs 	= strToLong(mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CONN_WAIT_INTERVAL_MS),0);
					long lJdbcTimeoutMs 		= strToLong(mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CONN_TIMEOUT_MS),0);
					
					if(lJdbcMaxConn>0)
					{
						dbmgr.setMaxDBConn((int)lJdbcMaxConn);
					}
					
					if(lJdbcWaitIntervalMs>0)
					{
						dbmgr.setWaitingIntervalMs(lJdbcWaitIntervalMs);
					}
					
					if(lJdbcTimeoutMs>0)
					{
						dbmgr.setTimeoutMs(lJdbcTimeoutMs);
					}
					
					if(lJdbcFetchSize>0)
					{
						dbmgr.setDBFetchSize((int)lJdbcFetchSize);
					}
					
					
				}catch(Exception ex)
				{
					throw new SQLException("Error initialize JDBC - "+aJdbcConfigKey, ex);
				}
				//
				int lconnpoolsize 		= -1;
				String sConnPoolSize 	= mapJdbcConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CONNPOOL);
				if(sConnPoolSize!=null)
				{
					try{
						lconnpoolsize = Integer.parseInt(sConnPoolSize);
						if(lconnpoolsize>-1)
						{
							dbmgr.setDBConnPoolSize(lconnpoolsize);
						}
					}
					catch(NumberFormatException ex){}
				}
			}
			
			if(dbmgr!=null)
			{
				dbmgr.setReferenceConfig(mapJdbcConfig);
				mapDBMgr.put(aJdbcConfigKey, dbmgr);
			}
		}
		return dbmgr;
	}
	
	private static long strToLong(String aString, long aDefaultVal)
	{
		long lValue = aDefaultVal;
		if(aString!=null)
		{
			try {
				lValue = Long.parseLong(aString);
			}catch(NumberFormatException ex)
			{
				lValue = aDefaultVal;
			}
		}
		return lValue;
	}
	
	public void reloadProps() throws Exception 
	{
		clearAll();
		
		if(jsoncrudConfig==null)
		{
			jsoncrudConfig 	= new JsonCrudConfig(config_prop_filename);
		}
		
		for(String sKey : jsoncrudConfig.getConfigCrudKeys())
		{
			if(!sKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD+"."))
			{
				
				if(sKey.startsWith(JsonCrudConfig._PROP_KEY_JDBC))
				{
					Map<String, String> map = jsoncrudConfig.getConfig(sKey);
					initNRegJdbcDBMgr(sKey, map);
				}
				
				//only process crud.xxx
				continue;
			}
			
			JdbcDBMgr dbmgr = null;
			Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(sKey);
			String sDBConfigName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
			//
			if(sDBConfigName!=null && sDBConfigName.trim().length()>0)
			{
				dbmgr = mapDBMgr.get(sDBConfigName);
				if(dbmgr==null)
				{
					Map<String, String> mapDBConfig = jsoncrudConfig.getConfig(sDBConfigName);
					
					if(mapDBConfig==null)
						throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid "+JsonCrudConfig._PROP_KEY_DBCONFIG+" - "+sDBConfigName);
									
					String sJdbcClassname = mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME);
					
					if(sJdbcClassname!=null)
					{
						dbmgr = initNRegJdbcDBMgr(sDBConfigName, mapDBConfig);
					}
					else
					{
						throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid "+JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME+" - "+sJdbcClassname);
					}
				}
			}
			
			if(dbmgr!=null)
			{
				Map<String, String> mapCrudJson2Col = new HashMap<String, String> ();
				Map<String, String> mapCrudCol2Json = new HashMap<String, String> ();
				Map<String, String> mapCrudJsonSql 	= new HashMap<String, String> ();
				
				for(String sCrudCfgKey : mapCrudConfig.keySet())
				{
					Matcher m = pattJsonColMapping.matcher(sCrudCfgKey);
					if(m.find())
					{
						String jsonname  = m.group(1);
						String dbcolname = mapCrudConfig.get(sCrudCfgKey);		
						mapCrudJson2Col.put(jsonname, dbcolname);
						mapCrudCol2Json.put(dbcolname, jsonname);
						continue;
					}
					//
					m = pattJsonSQL.matcher(sCrudCfgKey);
					if(m.find())
					{
						String jsonname = m.group(1);
						String sql 		= mapCrudConfig.get(sCrudCfgKey);
						
						if(sql==null) sql = "";
							else sql = sql.trim();
						
						if(sql.length()>0)
						{
							mapCrudJsonSql.put(jsonname, sql);
						}
						continue;
						
					}
					//
				}

				String sTableName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_TABLENAME);
				logger.log(Level.INFO,"[init] "+sKey+" - tablename:"+sTableName+" ... ");
				
				if(sTableName!=null && sTableName.trim().length()>0)
				{
					Map<String, DBColMeta> mapCols = cacheTableMetaDataBySQL(sKey, "SELECT * FROM "+sTableName+" WHERE 1=2");
					if(mapCols!=null)
					{	

						logger.log(Level.INFO, sKey+"."+sTableName+" : "+mapCols.size()+" cols meta loaded.");
						
						for(String sColName : mapCols.keySet())
						{
							//No DB Mapping configured
							if(mapCrudCol2Json.get(sColName)==null)
							{
								mapCrudJson2Col.put(sColName, sColName);
								mapCrudCol2Json.put(sColName, sColName);
							}
						}
					}
				}
				
				mapJson2ColName.put(sKey, mapCrudJson2Col);
				mapColName2Json.put(sKey, mapCrudCol2Json);
				mapJson2Sql.put(sKey, mapCrudJsonSql);
				//
			}
		}
	}
	
	private Map<String, DBColMeta> cacheTableMetaDataBySQL(String sKey, String aSQL) throws SQLException
	{
		Map<String, DBColMeta> mapCols = null;
		Map<String, DBColMeta> mapNewCols = getTableMetaDataBySQL(sKey, aSQL);
		if(mapNewCols!=null)
		{
			
			mapCols = mapTableCols.get(sKey);
			if(mapCols==null)
			{
				mapCols = mapNewCols;
			}
			else
			{
				mapCols.putAll(mapNewCols);
			}
			mapTableCols.put(sKey, mapCols);
		}
		return mapCols;
	}
	
	
	public Map<String, String[]> validateDataWithSchema(String aCrudKey, JSONObject aJsonData)
	{
		return validateDataWithSchema(aCrudKey, aJsonData, false);
	}
	
	public Map<String, String[]> validateDataWithSchema(String aCrudKey, JSONObject aJsonData, boolean isDebugMode)
	{
		Map<String, String[]> mapError = new HashMap<String, String[]>();
		if(aJsonData!=null)
		{
			Map<String, String> mapJsonToCol = mapJson2ColName.get(aCrudKey);
			StringBuffer sbErrInfo = new StringBuffer();
			
			Map<String, DBColMeta> mapCols = mapTableCols.get(aCrudKey);
			if(mapCols!=null && mapCols.size()>0)
			{
				for(String sJsonKey : aJsonData.keySet())
				{
					String sJsonColName = mapJsonToCol.get(sJsonKey);
					if(sJsonColName==null)
					{
						//skip not mapping found
						continue;
					}
					DBColMeta col = mapCols.get(sJsonColName);
					if(col!=null)
					{
						Object oVal = aJsonData.get(sJsonKey);
						List<String> listErr = new ArrayList<String>();
						
						////// Check if Nullable //////////
						if(!col.getColnullable())
						{
							if(oVal==JSONObject.NULL || oVal==null || oVal.toString().trim().length()==0)
							{
								sbErrInfo.setLength(0);
								sbErrInfo.append(JsonCrudConfig.ERRCODE_NOT_NULLABLE);								
								if(isDebugMode)
								{
									sbErrInfo.append(" - '").append(col.getColname()).append("' cannot be empty. ").append(col);
								}
								listErr.add(sbErrInfo.toString());
							}
						}

						if(oVal!=null && oVal!=JSONObject.NULL)
						{						
							////// Check Data Type //////////
							boolean isInvalidDataType = false;
							if(oVal instanceof String)
							{
								isInvalidDataType = !col.isString();
							}
							else if (oVal instanceof Boolean)
							{
								isInvalidDataType = (!col.isBit() && !col.isBoolean());
							}
							else if (oVal instanceof Long 
									|| oVal instanceof Double
									|| oVal instanceof Float
									|| oVal instanceof Integer)
							{
								isInvalidDataType = !col.isNumeric();
							}
							
							if(isInvalidDataType)
							{
								sbErrInfo.setLength(0);
								sbErrInfo.append(JsonCrudConfig.ERRCODE_INVALID_TYPE);								
								if(isDebugMode)
								{
									sbErrInfo.append(" - '").append(col.getColname()).append("' invalid type,");
									sbErrInfo.append(" expect:").append(col.getColtypename());
									sbErrInfo.append(" actual:").append(oVal.getClass().getSimpleName()).append(". ").append(col);
								}
								listErr.add(sbErrInfo.toString());
							}
	
							
							////// Check Data Size //////////
							if(oVal instanceof String && !isInvalidDataType)
							{
								String sVal = oVal.toString();
								if(sVal.length()>col.getColsize())
								{
									sbErrInfo.setLength(0);
									sbErrInfo.append(JsonCrudConfig.ERRCODE_EXCEED_SIZE);								
									if(isDebugMode)
									{
										sbErrInfo.append(" - '").append(col.getColname()).append("' exceed allowed size,");
										sbErrInfo.append(" expect:").append(col.getColsize());
										sbErrInfo.append(" actual:").append(sVal.length()).append(". ").append(col);
									}
									listErr.add(sbErrInfo.toString());
								}
							}
							///// Check if Data is autoincremental //////
							if(col.getColautoincrement())
							{
								// 
								sbErrInfo.setLength(0);
								sbErrInfo.append(JsonCrudConfig.ERRCODE_SYSTEM_FIELD);								
								if(isDebugMode)
								{
									sbErrInfo.append(" - '").append(col.getColname()).append("' not allowed (auto increment field). ").append(col);
								}
								listErr.add(sbErrInfo.toString());
							}
						}
						
						if(listErr.size()>0)
						{
						mapError.put(sJsonKey, listErr.toArray(new String[listErr.size()]));
						}
						
					}
				}
			}	
		}
		
		return mapError;
	}
	
	
	public JSONObject castJson2DBVal(String aCrudKey, JSONObject aDataObj)
	{
		if(aDataObj==null)
			return null;
		
		JSONObject jsonObj = new JSONObject();
		for(String sKey : aDataObj.keySet())
		{
			Object o = aDataObj.get(sKey);
			o = castJson2DBVal(aCrudKey, sKey, o);
			jsonObj.put(sKey, o);
		}
		
		return jsonObj;
	}
	
	public Object castJson2DBVal(String aCrudKey, String aJsonName, Object aVal)
	{
		if(aVal == null)
			return JSONObject.NULL;
		
		if(!(aVal instanceof String))
			return aVal;
		
		if(aJsonName.toLowerCase().indexOf(".in")>-1)
			return aVal;
		
		//only cast string value
		aJsonName = getJsonNameNoFilter(aJsonName);
		
		Object oVal = aVal;
		DBColMeta col = getDBColMetaByJsonName(aCrudKey, aJsonName);
		if(col!=null)
		{
			String sVal = String.valueOf(aVal);
			
			if(col.isNumeric())
			{
				if(sVal.indexOf('.')>-1)
					oVal = Double.parseDouble(sVal);
				else
					oVal = Long.parseLong(sVal);
			}
			else if(col.isBoolean() || col.isBit())
			{
				oVal = Boolean.parseBoolean(sVal);
			}
		}
		return oVal;
	}
	
	public DBColMeta getDBColMetaByColName(String aCrudKey, String aColName)
	{
		Map<String,DBColMeta> cols = mapTableCols.get(aCrudKey);
		for(DBColMeta col : cols.values())
		{
			if(col.getColname().equalsIgnoreCase(aColName))
			{
				return col;
			}
		}
		return null;
	}
	
	public DBColMeta getDBColMetaByJsonName(String aCrudKey, String aJsonName)
	{
		aJsonName = getJsonNameNoFilter(aJsonName);
		//
		Map<String,DBColMeta> cols = mapTableCols.get(aCrudKey);
		if(cols!=null)
		{
			Map<String,String> mapCol2Json = mapColName2Json.get(aCrudKey);
			if(mapCol2Json!=null && mapCol2Json.size()>0)
			for(DBColMeta col : cols.values())
			{
				String sColJsonName = mapCol2Json.get(col.getColname());
				if(sColJsonName!=null && sColJsonName.equalsIgnoreCase(aJsonName))
				{
					return col;
				}
			}
		}
		return null;
	}
	
	private Map<String, DBColMeta> getTableMetaDataBySQL(String aCrudKey, String aSQL) throws SQLException
	{
		if(aSQL==null || aSQL.length()==0)
			return null;
		
		if(mapSQLmeta.contains(aSQL))
			return null;
		
		Map<String, DBColMeta> mapDBColJson = new HashMap<String, DBColMeta>();
		String sSQL = aSQL;

		Map<String, String> mapConfig = jsoncrudConfig.getConfig(aCrudKey);
		String sJdbcKey = mapConfig.get("dbconfig");
		
		JdbcDBMgr jdbcMgr = mapDBMgr.get(sJdbcKey);
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		
		try{
			conn = jdbcMgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			rs = stmt.executeQuery();
			
			Map<String, String> mapColJsonName = mapColName2Json.get(aCrudKey);
			if(mapColJsonName==null)
				mapColJsonName = new HashMap<String,String>();
			
			ResultSetMetaData meta = rs.getMetaData();
			for(int i=0; i<meta.getColumnCount(); i++)
			{
				int idx = i+1;
				DBColMeta coljson = new DBColMeta();
				coljson.setColseq(idx);
				coljson.setTablename(meta.getTableName(idx));
				coljson.setColname(meta.getColumnLabel(idx));
				coljson.setColclassname(meta.getColumnClassName(idx));
				coljson.setColtypename(meta.getColumnTypeName(idx));
				coljson.setColtype(String.valueOf(meta.getColumnType(idx)));
				coljson.setColsize(meta.getColumnDisplaySize(idx));
				coljson.setColnullable(ResultSetMetaData.columnNullable == meta.isNullable(idx));
				coljson.setColautoincrement(meta.isAutoIncrement(idx));
				//
				String sJsonName = mapColJsonName.get(coljson.getColname());
				if(sJsonName!=null)
				{
					coljson.setJsonname(sJsonName);
				}
				//
				mapDBColJson.put(coljson.getColname(), coljson);
			}
		}
		finally
		{
			jdbcMgr.closeQuietly(conn, stmt, rs);
		}
		
		if(mapDBColJson.size()==0)
			return null;
		else
		{
			mapSQLmeta.add(aSQL);
			return mapDBColJson;
		}
		
	}
	
	public boolean isDebugMode(String aCrudConfigKey)
	{
		Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(aCrudConfigKey);
		if(mapCrudConfig!=null)
		{
			return Boolean.parseBoolean(mapCrudConfig.get(JsonCrudConfig._PROP_KEY_DEBUG));
		}
		return false;
	}
	
	private String listParamsToString(List listParams)
	{
		if(listParams!=null)
			return listParamsToString(listParams.toArray(new Object[listParams.size()]));
		else
			return null;
	}
	
	private String listParamsToString(Object[] aObjParams)
	{
		JSONArray jsonArr   = new JSONArray();
		
		if(aObjParams!=null && aObjParams.length>0)
		{
			for(int i=0; i<aObjParams.length; i++)
			{
				Object o = aObjParams[i];
				
				if(o.getClass().isArray())
				{
					Object[] objs = (Object[]) o;
					JSONArray jsonArr2 	= new JSONArray();
					for(int j=0; j<objs.length; j++)
					{
						jsonArr2.put(objs[j].toString());
					}
					jsonArr.put(jsonArr2);
				}
				else
				{
					jsonArr.put(o);
				}
			}
		}
		return jsonArr.toString();
	}
	
	private long updateChildObject(JdbcDBMgr aDBMgr, String aObjInsertSQL, List<Object[]> aListParams) throws JsonCrudException
	{
		long lAffectedRow 			= 0;
		String sChildTableName 	 	= null;
		String sChildTableFields 	= null;
		
		Matcher m = pattInsertSQLtableFields.matcher(aObjInsertSQL.toLowerCase());
		if(m.find())
		{
			sChildTableName 	= m.group(1);
			sChildTableFields 	= m.group(2);
		}
		
		StringBuffer sbWhere = new StringBuffer();
		StringTokenizer tk = new StringTokenizer(sChildTableFields,",");
		while(tk.hasMoreTokens())
		{
			if(sbWhere.length()>0)
			{
				sbWhere.append(" AND ");
			}
			else
			{
				sbWhere.append("(");
			}
			String sField = tk.nextToken();
			sbWhere.append(sField).append(" = ? ");
		}
		sbWhere.append(")");

		if(aListParams!=null && aListParams.size()>0)
		{
			/////
			
			List<Object> listFlattenParam = new ArrayList<Object>();
			//
			StringBuffer sbObjSQL2 	= new StringBuffer();
			sbObjSQL2.append("SELECT * FROM ").append(sChildTableName).append(" WHERE 1=1 ");
			sbObjSQL2.append(" AND ").append(sbWhere);
			//
			List<Object[]> listParams_new = new ArrayList<Object[]>(); 
			for(Object[] obj2 : aListParams)
			{
				for(Object o : obj2)
				{
					if((o instanceof String) && o.toString().equals(""))
					{
						o = null;
					}
					listFlattenParam.add(o);
				}
				
				try {
					if(aDBMgr.getQueryCount(sbObjSQL2.toString(), obj2)==0)
					{
						listParams_new.add(obj2);
					}
				} catch (SQLException e) {
					throw new JsonCrudException(
							JsonCrudConfig.ERRCODE_SQLEXCEPTION, "sql:"+sbObjSQL2.toString()+", params:"+listParamsToString(obj2), e);
				}
			}
			aListParams.clear();
			aListParams.addAll(listParams_new);
			listParams_new.clear();
			
			aObjInsertSQL = aObjInsertSQL.replaceAll("\\{.+?\\}", "?");
			try{
				lAffectedRow += aDBMgr.executeBatchUpdate(aObjInsertSQL, aListParams);
			}
			catch(SQLException sqlEx)
			{
				throw new JsonCrudException(
						JsonCrudConfig.ERRCODE_SQLEXCEPTION, "sql:"+aObjInsertSQL+", params:"+listParamsToString(aListParams), sqlEx);
			}
		}
		return lAffectedRow;
	}
	
	
	private List<Object[]> getSubQueryParams(Map<String, String> aCrudCfgMap, JSONObject aJsonParentData, String aJsonName) throws JsonCrudException
	{
		String sPrefix 		= "jsonattr."+aJsonName+".";
		String sObjSQL 		= aCrudCfgMap.get(sPrefix+JsonCrudConfig._PROP_KEY_CHILD_INSERTSQL);
		String sObjMapping 	= aCrudCfgMap.get(sPrefix+JsonCrudConfig._PROP_KEY_CHILD_MAPPING);
		
		String sObjKeyName = null;
		String sObjValName = null;
		
		List<Object[]> listAllParams 	= new ArrayList<Object[]>();
		List<Object>  listParamObj 		= new ArrayList<Object>(); 
		
		if(sObjSQL!=null && sObjMapping!=null)
		{
			Object obj				= aJsonParentData.get(aJsonName);
			Iterator iter 			= null;
			boolean isKeyValPair 	= (obj instanceof JSONObject);
			JSONObject jsonKeyVal   = null;
			
			if(isKeyValPair)
			{
				//Key-Value pair
				
				JSONObject jsonObjMapping = new JSONObject(sObjMapping);
				sObjKeyName = jsonObjMapping.keySet().iterator().next();
				sObjValName = (String) jsonObjMapping.get(sObjKeyName);

				jsonKeyVal = (JSONObject) obj;
				iter = jsonKeyVal.keySet().iterator();
				
			}
			else if(obj instanceof JSONArray)
			{
				//Single level Array
				
				JSONArray jsonObjMapping = new JSONArray(sObjMapping);
				sObjKeyName = (String) jsonObjMapping.iterator().next();
				
				JSONArray jsonArr 	= (JSONArray) obj;
				iter = jsonArr.iterator();
			}
			
			while(iter.hasNext())
			{
				String sKey = (String) iter.next();
				
				listParamObj.clear();
				Matcher m = pattSQLjsonname.matcher(sObjSQL);
				while(m.find())
				{
					String sBracketJsonName = m.group(1);
					if(aJsonParentData.has(sBracketJsonName))
					{
						listParamObj.add(aJsonParentData.get(sBracketJsonName));
					}
					else if(sBracketJsonName.equals(sObjKeyName))
					{
						listParamObj.add(sKey);
					}
					else if(jsonKeyVal!=null && sBracketJsonName.equals(sObjValName))
					{
						listParamObj.add(jsonKeyVal.get(sKey));
					}
				}
				listAllParams.add(listParamObj.toArray(new Object[listParamObj.size()]));
			}
		}
		
		if(sObjKeyName==null)
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "No object mapping found ! - "+aJsonName);
		///		
		
		return listAllParams;
	}
	
	private boolean isEmptyJson(String aJsonString)
	{
		if(aJsonString==null || aJsonString.trim().length()==0)
			return true;
		aJsonString = aJsonString.replaceAll("\\s", "");
		
		while(aJsonString.startsWith("{") || aJsonString.startsWith("["))
		{
			if(aJsonString.length()==2)
				return true;
			
			if("{\"\":\"\"}".equalsIgnoreCase(aJsonString))
			{
				return true;
			}
			
			aJsonString = aJsonString.substring(1, aJsonString.length()-1);
		}
				
		return false;
	}
	
	private String getJsonNameNoFilter(String aJsonName)
	{
		int iPos = aJsonName.indexOf(".");
		if(iPos >-1)
		{
			aJsonName = aJsonName.substring(0,iPos);
		}
		return aJsonName;
		
	}
	
	public JSONObject convertCol2Json(String aCrudKey, JSONObject aJSONObject)
	{
		if(!aCrudKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD+"."))
		{
			aCrudKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		}
		
		Map<String, String> mapCrudCol2Json = mapColName2Json.get(aCrudKey);
		
		if(aJSONObject==null || mapCrudCol2Json==null || mapCrudCol2Json.size()==0)
		{
			return aJSONObject;
		}
		
		JSONObject jsonConverted = new JSONObject();

		for(String sColName : aJSONObject.keySet())
		{
			String sMappedKey = mapCrudCol2Json.get(sColName);
			if(sMappedKey==null)
				sMappedKey = sColName;
			
			Object obj = aJSONObject.get(sColName);
			
			if(obj instanceof JSONObject)
			{
				obj = convertCol2Json(aCrudKey, (JSONObject) obj);
			}
			else if(obj instanceof JSONArray)
			{
				JSONArray jArrNew = new JSONArray();
				JSONArray jArr = (JSONArray) obj;
				for(int i=0; i<jArr.length(); i++)
				{
					Object obj2 = jArr.get(i);
					if(obj2 instanceof JSONObject)
					{
						obj2 = convertCol2Json(aCrudKey, (JSONObject) obj2);
					}
					jArrNew.put(obj2);
				}
				obj = jArrNew;
			}
			
			jsonConverted.put(sMappedKey, obj);
		}
		
		return jsonConverted;
	}
	
	public JdbcDBMgr getJdbcMgr(String aJdbcConfigName)
	{
		
		return mapDBMgr.get(aJdbcConfigName);		
	}
}
