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
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import hl.common.JdbcDBMgr;

public class CRUDMgr {
	
	private final static String JSONFILTER_FROM 				= "from";
	private final static String JSONFILTER_TO 					= "to";
	private final static String JSONFILTER_STARTWITH			= "startwith";
	private final static String JSONFILTER_ENDWITH				= "endwith";
	private final static String JSONFILTER_CONTAIN				= "contain";
	private final static String SQLLIKE_WILDCARD				= "%";
	private final static String JSONVAL_WILDCARD				= "*";
	private final static String JSONFILTER_CASE_INSENSITIVE		= "ci";
	
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
	
	public final static String _PAGINATION_CONFIGKEY = "list.pagination";
	public String _LIST_META 		= "meta";
	public String _LIST_RESULT 		= "result";
	public String _LIST_TOTAL 		= "total";
	public String _LIST_FETCHSIZE 	= "fetchsize";
	public String _LIST_START 		= "start";
	public String _LIST_ORDERBY 	= "orderby";
	public String _LIST_ORDERDESC	= "orderdesc";
	
	public final static String _DB_VALIDATION_ERRCODE_CONFIGKEY = "dbschema.validation_errcode";
	public static String ERRCODE_NOT_NULLABLE 	= "not_nullable";
	public static String ERRCODE_EXCEED_SIZE 	= "exceed_size";
	public static String ERRCODE_INVALID_TYPE	= "invalid_type";
	public static String ERRCODE_SYSTEM_FIELD	= "system_field";
	
	public CRUDMgr()
	{
		config_prop_filename = null;
		init();
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
		pattJsonNameFilter 	= Pattern.compile("([a-zA-Z_-]+?)\\.("
					+JSONFILTER_FROM+"|"+JSONFILTER_TO+"|"
					+JSONFILTER_STARTWITH+"|"+JSONFILTER_ENDWITH+"|"+JSONFILTER_CONTAIN+"|"
					+JSONFILTER_CASE_INSENSITIVE+")"
					+"(?:\\.("+JSONFILTER_CASE_INSENSITIVE+"))?");
		
		try {
			reloadProps();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		initPaginationConfig();
		initValidationErrCodeConfig();
	}

	private void initValidationErrCodeConfig()
	{
		Map<String, String> mapErrCodes = jsoncrudConfig.getConfig(_DB_VALIDATION_ERRCODE_CONFIGKEY);
		
		if(mapErrCodes!=null && mapErrCodes.size()>0)
		{
			String sMetaKey = null;
			
			sMetaKey = mapErrCodes.get(ERRCODE_EXCEED_SIZE);
			if(sMetaKey!=null)
				ERRCODE_EXCEED_SIZE = sMetaKey;
			
			sMetaKey = mapErrCodes.get(ERRCODE_INVALID_TYPE);
			if(sMetaKey!=null)
				ERRCODE_INVALID_TYPE = sMetaKey;

			sMetaKey = mapErrCodes.get(ERRCODE_NOT_NULLABLE);
			if(sMetaKey!=null)
				ERRCODE_NOT_NULLABLE = sMetaKey;
			
			sMetaKey = mapErrCodes.get(ERRCODE_SYSTEM_FIELD);
			if(sMetaKey!=null)
				ERRCODE_SYSTEM_FIELD = sMetaKey;
		}		
		
	}

	private void initPaginationConfig()
	{
		Map<String, String> mapPagination = jsoncrudConfig.getConfig(_PAGINATION_CONFIGKEY);
		
		if(mapPagination!=null && mapPagination.size()>0)
		{
			String sMetaKey = null;
			
			sMetaKey = mapPagination.get(_LIST_META);
			if(sMetaKey!=null)
				_LIST_META = sMetaKey;
			
			sMetaKey = mapPagination.get(_LIST_RESULT);
			if(sMetaKey!=null)
				_LIST_RESULT = sMetaKey;
			
			sMetaKey = mapPagination.get(_LIST_TOTAL);
			if(sMetaKey!=null)
				_LIST_TOTAL = sMetaKey;
	
			sMetaKey = mapPagination.get(_LIST_FETCHSIZE);
			if(sMetaKey!=null)
				_LIST_FETCHSIZE = sMetaKey;
			
			sMetaKey = mapPagination.get(_LIST_START);
			if(sMetaKey!=null)
				_LIST_START = sMetaKey;
					
			sMetaKey = mapPagination.get(_LIST_ORDERBY);
			if(sMetaKey!=null)
				_LIST_ORDERBY = sMetaKey;
			
			sMetaKey = mapPagination.get(_LIST_ORDERDESC);
			if(sMetaKey!=null)
				_LIST_ORDERDESC = sMetaKey;
		}		
		
	}
	
	public JSONObject create(String aCrudKey, JSONObject aDataJson) throws Exception
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
			throw new Exception("Invalid crud configuration key ! - "+aCrudKey);
		
		aDataJson = castJson2DBVal(aCrudKey, aDataJson);
		
		StringBuffer sbColName 	= new StringBuffer();
		StringBuffer sbParams 	= new StringBuffer();
		
		StringBuffer sbRollbackParentSQL = new StringBuffer();
		
		List<Object> listValues = new ArrayList<Object>();
		
		List<String> listUnmatchedJsonName = new ArrayList<String>();
		
		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		
		
		Map<String, String> mapCrudJsonCol = mapJson2ColName.get(aCrudKey);
		for(String sJsonName : aDataJson.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			if(sColName!=null)
			{
				//
				if(sbColName.length()>0)
				{
					sbColName.append(",");
					sbParams.append(",");
				}
				else if(sbRollbackParentSQL.length()==0)
				{
					sbRollbackParentSQL.append("DELETE FROM ").append(sTableName).append(" WHERE 1=1 ");					
				}
				sbRollbackParentSQL.append(" AND ").append(sColName).append(" = ? ");
				sbColName.append(sColName);
				sbParams.append("?");
				//
				listValues.add(aDataJson.get(sJsonName));
			}
			else
			{
				listUnmatchedJsonName.add(sJsonName);
			}
		}
		

		String sSQL 		= "INSERT INTO "+sTableName+"("+sbColName.toString()+") values ("+sbParams.toString()+")";
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		long lAffectedRow = 0;
				
		try {
			lAffectedRow = dbmgr.executeUpdate(sSQL, listValues);	
		}
		catch(Throwable ex)
		{
			throw new Exception("sql:"+sSQL+", params:"+listParamsToString(listValues), ex);
		}

		
		if(lAffectedRow>0)
		{
			//child create
			if(listUnmatchedJsonName.size()>0)
			{
				JSONArray jsonArrReturn = retrieve(aCrudKey, aDataJson);
				
				for(int i=0 ; i<jsonArrReturn.length(); i++  )
				{
					JSONObject jsonReturn = jsonArrReturn.getJSONObject(i);
							
					//merging json obj
					for(String sDataJsonKey : aDataJson.keySet())
					{
						if(jsonReturn.has(sDataJsonKey))
						{
							jsonReturn.put(sDataJsonKey, aDataJson.get(sDataJsonKey));
						}
					}
					
					for(String sJsonName2 : listUnmatchedJsonName)
					{
						List<Object[]> listParams2 	= getSubQueryParams(map, jsonReturn, sJsonName2);
						String sObjInsertSQL 		= map.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_OBJ_SQL);
						
						long lupdatedRow = 0;
						
						try {
							lupdatedRow = updateChildObject(dbmgr, sObjInsertSQL, listParams2);
						}
						catch(Throwable ex)
						{
							try {
								//rollback parent
								long lRollbackRow = dbmgr.executeUpdate(sbRollbackParentSQL.toString(), listValues);
								if(lAffectedRow!=lRollbackRow)
								{
									throw new Exception("Record fail to Rollback!");
								}
							}
							catch(Throwable ex2)
							{
								throw new Exception("[Rollback Failed], parent:[sql:"+sbRollbackParentSQL.toString()+",params:"+listParamsToString(listValues)+"], child:[sql:"+sObjInsertSQL+",params:"+listParamsToString(listParams2)+"]", ex);
							}
							
							throw new Exception("[Rollback Success] : child : sql:"+sObjInsertSQL+", params:"+listParamsToString(listParams2), ex);
						}
							
					}
				}
			}			
			
			JSONArray jsonArray = retrieve(aCrudKey, aDataJson);
			return (JSONObject) jsonArray.get(0);
		}
		else
		{
			return null;
		}
	}
	
	public JSONObject retrieveFirst(String aCrudKey, JSONObject aWhereJson) throws Exception
	{
		JSONArray jsonArr = retrieve(aCrudKey, aWhereJson);
		if(jsonArr!=null && jsonArr.length()>0)
		{
			return (JSONObject) jsonArr.get(0);
		}
		return null;
	}
	
	public JSONArray retrieve(String aCrudKey, JSONObject aWhereJson) throws Exception
	{
		JSONObject json = retrieve(aCrudKey, aWhereJson, 0, 0, null, false);
		if(json==null)
		{
			return new JSONArray();
		}
		return (JSONArray) json.get(_LIST_RESULT);
	}
	
	public JSONObject retrieve(String aCrudKey, JSONObject aWhereJson, 
			int aStartFrom, int aFetchSize, String[] aOrderBy, boolean isOrderDesc) throws Exception
	{
		JSONObject jsonReturn 		= null;

		aWhereJson = castJson2DBVal(aCrudKey, aWhereJson);
		
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
			throw new Exception("Invalid crud configuration key ! - "+aCrudKey);
		
		List<Object> listValues 			= new ArrayList<Object>();		
		Map<String, String> mapCrudJsonCol 	= mapJson2ColName.get(aCrudKey);

		// WHERE
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sOrgJsonName : aWhereJson.keySet())
		{
			boolean isCaseInSensitive = false;
			String sOperator 	= " = ";
			String sJsonName 	= sOrgJsonName;
			Object oJsonValue 	= aWhereJson.get(sOrgJsonName);
			
			if(sJsonName.indexOf(".")>-1)
			{
				Matcher m = pattJsonNameFilter.matcher(sJsonName);
				if(m.find())
				{
					sJsonName = m.group(1);
					String sJsonOperator = m.group(2);
					String sJsonCI = m.group(3);
					
					if(sJsonCI!=null || JSONFILTER_CASE_INSENSITIVE.equals(sJsonOperator))
					{
						isCaseInSensitive = true;
					}
					
					if(JSONFILTER_FROM.equals(sJsonOperator))
					{
						sOperator = " >= ";
					}
					else if(JSONFILTER_TO.equals(sJsonOperator))
					{
						sOperator = " <= ";
					}
					else if(oJsonValue!=null && oJsonValue instanceof String)
					{
						if(JSONFILTER_STARTWITH.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = oJsonValue+SQLLIKE_WILDCARD;
						}
						else if (JSONFILTER_ENDWITH.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = SQLLIKE_WILDCARD+oJsonValue;
						}
						else if (JSONFILTER_CONTAIN.equals(sJsonOperator))
						{
							sOperator = " like ";
							oJsonValue = SQLLIKE_WILDCARD+oJsonValue+SQLLIKE_WILDCARD;
						}
					}
				}
			}
			
			if(oJsonValue!=null && (oJsonValue instanceof String) && (oJsonValue.toString().indexOf(JSONVAL_WILDCARD)>-1))
			{
				sOperator = " like ";
				oJsonValue = oJsonValue.toString().replaceAll(Pattern.quote(JSONVAL_WILDCARD), SQLLIKE_WILDCARD);
				
			}
			
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			if(sColName!=null)
			{
				if(isCaseInSensitive && (oJsonValue instanceof String))
				{
					sbWhere.append(" AND UPPER(").append(sColName).append(") ").append(sOperator).append(" UPPER(?) ");
				}
				else
				{
					sbWhere.append(" AND ").append(sColName).append(sOperator).append(" ? ");
				}
				// 
				oJsonValue = castJson2DBVal(aCrudKey, sJsonName, oJsonValue);
				listValues.add(oJsonValue);
			}
		}
		
		StringBuffer sbOrderBy = new StringBuffer();
		if(aOrderBy!=null && aOrderBy.length>0)
		{
			for(String sJsonAttr : aOrderBy)
			{
				String sOrderColName = mapCrudJsonCol.get(sJsonAttr);
				
				if(sOrderColName!=null)
				{
					if(sbOrderBy.length()>0)
					{
						sbOrderBy.append(",");
					}
					sbOrderBy.append(sOrderColName);
				}
			}
			if(sbOrderBy.length()>0)
			{
				sbWhere.append(" ORDER BY ").append(sbOrderBy.toString());
				if(isOrderDesc)
				{
					sbWhere.append(" DESC ");
				}
			}
		}

		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		String sSQL 		= "SELECT * FROM "+sTableName+" WHERE 1=1 "+sbWhere.toString();
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		

		JSONArray jsonArr = new JSONArray();
		try{
			
			Map<String, String> mapCrudCol2Json = mapColName2Json.get(aCrudKey);
			Map<String, String> mapCrudSql 		= mapJson2Sql.get(aCrudKey);
			
			conn = dbmgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			stmt = JdbcDBMgr.setParams(stmt, listValues);
			rs   = stmt.executeQuery();
			
			ResultSetMetaData meta 	= rs.getMetaData();
			long lTotalResult 		= 0;
			while(rs.next())
			{	
				lTotalResult++;
				
				if(lTotalResult-1 < aStartFrom)
					continue;
				
				JSONObject jsonOnbj = new JSONObject();
				
				for(int i=0; i<meta.getColumnCount(); i++)
				{
					String sColName = meta.getColumnLabel(i+1);
					
					String sJsonName = mapCrudCol2Json.get(sColName);
					if(sJsonName==null)
						sJsonName = sColName;
					jsonOnbj.put(sJsonName, rs.getObject(sColName));
				}
				
				if(mapCrudSql.size()>0)
				{
					int iTotalCols = 0;
					for(String sJsonName : mapCrudSql.keySet())
					{
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
							
							Connection conn2 	= null;
							PreparedStatement stmt2	= null;
							ResultSet rs2 = null;
							
							JSONArray jsonArr2 	= new JSONArray();
							JSONObject json2 	= null;
							try{
								conn2 = dbmgr.getConnection();
								stmt2 = conn2.prepareStatement(sSQL2);
								stmt2 = JdbcDBMgr.setParams(stmt2, listParams2);
								rs2   = stmt2.executeQuery();
								iTotalCols = rs2.getMetaData().getColumnCount();
								while(rs2.next())
								{
									Object o = null;
									String s = null;
									
									switch(iTotalCols)
									{
										case 1 : 
											o = rs2.getObject(1);
											if(o!=null)
												jsonArr2.put(o);
											break;
										case 2 : 
											s = rs2.getString(1);
											o = rs2.getObject(2);
											if(o!=null)
											{
												if(json2==null)
													json2 = new JSONObject();
												json2.put(s, o);
											}
											break;
										default :
											throw new Exception("Only 1 or 2 return columns from subquery are supported !");
									}
								}
							}catch(SQLException sqlEx)
							{
								throw new Exception("sql:"+sSQL2+", params:"+listParamsToString(listParams2), sqlEx);
							}
							finally{
								dbmgr.closeQuietly(conn2, stmt2, rs2);
							}
							
							if(json2!=null)
								jsonOnbj.put(sJsonName, json2);
							else
								jsonOnbj.put(sJsonName, jsonArr2);
						}
					}
				}
				
				jsonArr.put(jsonOnbj);
				
				if(aFetchSize>0 && jsonArr.length()>=aFetchSize)
				{
					break;
				}
			}
			
			while(rs.next()){
				lTotalResult++;
			}
			
			if(jsonArr.length()>0)
			{
				jsonReturn = new JSONObject();
				jsonReturn.put(_LIST_RESULT, jsonArr);
				//
				JSONObject jsonMeta = new JSONObject();
				jsonMeta.put(_LIST_TOTAL, lTotalResult);
				jsonMeta.put(_LIST_START, aStartFrom);
				//
				if(aFetchSize>0)
					jsonMeta.put(_LIST_FETCHSIZE, aFetchSize);
				//
				if(aOrderBy!=null)
				{
					StringBuffer sbOrderBys = new StringBuffer();
					for(String sOrderBy : aOrderBy)
					{
						if(sbOrderBys.length()>0)
							sbOrderBys.append(",");
						sbOrderBys.append(sOrderBy);
					}
					if(sbOrderBys.length()>0)
						jsonMeta.put(_LIST_ORDERBY, sbOrderBys.toString());
				}
				//
				jsonMeta.put(_LIST_ORDERDESC, isOrderDesc);
				//
				jsonReturn.put(_LIST_META, jsonMeta);
			}
			
		}
		catch(SQLException sqlEx)
		{
			throw new Exception("sql:"+sSQL+", params:"+listParamsToString(listValues), sqlEx);
		}
		finally
		{
			dbmgr.closeQuietly(conn, stmt, rs);
		}
		
		return jsonReturn;
	}	
	
	public JSONArray update(String aCrudKey, JSONObject aDataJson, JSONObject aWhereJson) throws Exception
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map.size()==0)
			throw new Exception("Invalid crud configuration key ! - "+aCrudKey);

		aDataJson 	= castJson2DBVal(aCrudKey, aDataJson);
		aWhereJson 	= castJson2DBVal(aCrudKey, aWhereJson);
		
		List<Object> listValues 			= new ArrayList<Object>();
		Map<String, String> mapCrudJsonCol 	= mapJson2ColName.get(aCrudKey);

		List<String> listUnmatchedJsonName	= new ArrayList<String>();
		//SET
		StringBuffer sbSets 	= new StringBuffer();
		for(String sJsonName : aDataJson.keySet())
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
				listValues.add(aDataJson.get(sJsonName));
			}
			else
			{
				listUnmatchedJsonName.add(sJsonName);
			}
		}
		// WHERE
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sJsonName : aWhereJson.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			if(sColName==null)
			{
				throw new Exception("Missing Json to dbcol mapping ("+sJsonName+":"+aWhereJson.get(sJsonName)+") ! - "+aCrudKey);
			}
			
			sbWhere.append(" AND ").append(sColName).append(" = ? ");
			//
			listValues.add(aWhereJson.get(sJsonName));
		}
		
		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		String sSQL			= "UPDATE "+sTableName+" SET "+sbSets.toString()+" WHERE 1=1 "+sbWhere.toString();
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		long lAffectedRow 	= 0;
		long lAffectedRow2 	= 0;
		
		try{
			
			if(sbSets.length()>0)
			{
				lAffectedRow = dbmgr.executeUpdate(sSQL, listValues);
			}
			
			if(lAffectedRow>0 || sbSets.length()==0)
			{
				//child update
				
				JSONArray jsonArrReturn = retrieve(aCrudKey, aWhereJson);
				
				for(int i=0 ; i<jsonArrReturn.length(); i++  )
				{
					JSONObject jsonReturn = jsonArrReturn.getJSONObject(i);
							
					//merging json obj
					for(String sDataJsonKey : aDataJson.keySet())
					{
						if(jsonReturn.has(sDataJsonKey))
						{
							jsonReturn.put(sDataJsonKey, aDataJson.get(sDataJsonKey));
						}
					}
					
					for(String sJsonName2 : listUnmatchedJsonName)
					{
						List<Object[]> listParams2 	= getSubQueryParams(map, jsonReturn, sJsonName2);
						String sObjInsertSQL 		= map.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_OBJ_SQL);
						
						lAffectedRow2 += updateChildObject(dbmgr, sObjInsertSQL, listParams2);
					}
				}
			}
		}
		catch(SQLException sqlEx) 
		{
			throw new Exception("sql:"+sSQL+", params:"+listParamsToString(listValues), sqlEx);
		}
		
		if(lAffectedRow>0 || lAffectedRow2>0)
		{
			JSONArray jsonArray = retrieve(aCrudKey, aWhereJson);
			return jsonArray;
		}
		else
		{
			return null;
		}
	}
	
	public JSONArray delete(String aCrudKey, JSONObject aWhereJson) throws Exception
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
			throw new Exception("Invalid crud configuration key ! - "+aCrudKey);
		
		aWhereJson = castJson2DBVal(aCrudKey, aWhereJson);
		
		List<Object> listValues 		= new ArrayList<Object>();
		Map<String, String> mapCrudJsonCol = mapJson2ColName.get(aCrudKey);
		
		StringBuffer sbWhere 	= new StringBuffer();
		for(String sJsonName : aWhereJson.keySet())
		{
			String sColName = mapCrudJsonCol.get(sJsonName);
			//
			sbWhere.append(" AND ").append(sColName).append(" = ? ");
			//
			listValues.add(aWhereJson.get(sJsonName));
		}
		
		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);
		boolean isDebug		= "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_DEBUG)); 
		String sSQL 		= "DELETE FROM "+sTableName+" WHERE 1=1 "+sbWhere.toString();
		
		String sJdbcName = map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr = mapDBMgr.get(sJdbcName);
		
		JSONArray jsonArray = null;
		

		jsonArray = retrieve(aCrudKey, aWhereJson);
		
		if(jsonArray.length()>0)
		{
			long lAffectedRow = 0;
			try {
				lAffectedRow = dbmgr.executeUpdate(sSQL, listValues);
			}
			catch(SQLException sqlEx)
			{
				throw new Exception("sql:"+sSQL+", params:"+listParamsToString(listValues), sqlEx);
			}
			
			if(lAffectedRow>0)
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

	public Map<String, String> getAllConfig() 
	{
		return jsoncrudConfig.getAllConfig();
	}
	
	public void reloadProps() throws Exception
	{
		clearAll();
		
		if(jsoncrudConfig==null)
		{
			jsoncrudConfig 	= new JsonCrudConfig(config_prop_filename);
		}
		
		for(String sKey : jsoncrudConfig.getConfigKeys())
		{
			if(!sKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD+"."))
			{
				//only process crud.xxx
				continue;
			}
			
			Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(sKey);
			String sDBConfigName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
			//
			JdbcDBMgr dbmgr = mapDBMgr.get(sDBConfigName);
			if(dbmgr==null)
			{
				Map<String, String> mapDBConfig = jsoncrudConfig.getConfig(sDBConfigName);
				
				if(mapDBConfig==null)
					throw new Exception("Invalid "+JsonCrudConfig._PROP_KEY_DBCONFIG+" - "+sDBConfigName);
				
				String sJdbcClassname = mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME);
				
				if(sJdbcClassname!=null)
				{
					dbmgr = new JdbcDBMgr(
							sJdbcClassname,
							mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_URL),
							mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_UID),
							mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_PWD));
					//
					int lconnpoolsize 		= -1;
					String sConnPoolSize 	= mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CONNPOOL);
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
					mapDBMgr.put(sDBConfigName, dbmgr);
				}
				else
				{
					throw new Exception("Invalid "+JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME+" - "+sJdbcClassname);
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
				mapJson2ColName.put(sKey, mapCrudJson2Col);
				mapColName2Json.put(sKey, mapCrudCol2Json);
				mapJson2Sql.put(sKey, mapCrudJsonSql);
				//

				String sTableName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_TABLENAME);
				System.out.print("[init] "+sKey+" - "+sTableName+" ... ");
				Map<String, DBColMeta> mapCols = getTableMetaData(sKey, dbmgr, sTableName);
				if(mapCols!=null)
				{
					System.out.println(mapCols.size()+" cols meta loaded.");
					mapTableCols.put(sKey, mapCols);
				}
			}
		}
	}
	
	public Map<String, String> validateDataWithSchema(String aCrudKey, JSONObject aJsonData)
	{
		return validateDataWithSchema(aCrudKey, aJsonData, false);
	}
	
	public Map<String, String> validateDataWithSchema(String aCrudKey, JSONObject aJsonData, boolean isDebugMode)
	{
		Map<String, String> mapError = new HashMap<String, String>();
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
						
						////// Check if Nullable //////////
						if(!col.getColnullable())
						{
							if(oVal==null || oVal.toString().trim().length()==0)
							{
								sbErrInfo.setLength(0);
								sbErrInfo.append(ERRCODE_NOT_NULLABLE);								
								if(isDebugMode)
								{
									sbErrInfo.append(" - '").append(col.getColname()).append("' cannot be empty. ").append(col);
								}
								mapError.put(sJsonKey, sbErrInfo.toString());
							}
						}

						if(oVal==null)
							continue;
						
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
							sbErrInfo.append(ERRCODE_INVALID_TYPE);								
							if(isDebugMode)
							{
								sbErrInfo.append(" - '").append(col.getColname()).append("' invalid type, expect:").append(col.getColtypename()).append(" actual:").append(oVal.getClass().getSimpleName()).append(". ").append(col);
							}
							mapError.put(sJsonKey, sbErrInfo.toString());
						}

						
						////// Check Data Size //////////
						String sVal = oVal.toString();
						if(sVal.length()>col.getColsize())
						{
							sbErrInfo.setLength(0);
							sbErrInfo.append(ERRCODE_EXCEED_SIZE);								
							if(isDebugMode)
							{
								sbErrInfo.append(" - '").append(col.getColname()).append("' exceed allowed size, expect:").append(col.getColsize()).append(" actual:").append(sVal.length()).append(". ").append(col);
							}
							mapError.put(sJsonKey, sbErrInfo.toString());
						}
						
						///// Check if Data is autoincremental //////
						if(col.getColautoincrement())
						{
							// 
							sbErrInfo.setLength(0);
							sbErrInfo.append(ERRCODE_SYSTEM_FIELD);								
							if(isDebugMode)
							{
								sbErrInfo.append(" - '").append(col.getColname()).append("' not allowed (auto increment field). ").append(col);
							}
							mapError.put(sJsonKey, sbErrInfo.toString());
						}
						
					}
				}
			}	
		}
		
		return mapError;
	}
	
	
	public JSONObject castJson2DBVal(String aCrudKey, JSONObject jsonObj)
	{
		if(jsonObj==null)
			return null;
		
		for(String sKey : jsonObj.keySet())
		{
			Object o = jsonObj.get(sKey);
			o = castJson2DBVal(aCrudKey, sKey, o);
			jsonObj.put(sKey, o);
		}
		
		return jsonObj;
	}
	
	public Object castJson2DBVal(String aCrudKey, String aJsonName, Object aVal)
	{
		if(!(aVal instanceof String))
			return aVal;
		
		//only cast string value
		aJsonName = getJsonNameNoFilter(aJsonName);
		
		Object oVal = aVal;
		DBColMeta col = getDBColMetaByJsonName(aCrudKey, aJsonName);
		if(col!=null)
		{
			if(col.isNumeric())
			{
				oVal = Long.parseLong(String.valueOf(aVal));
			}
			else if(col.isBoolean() || col.isBit())
			{
				oVal = Boolean.parseBoolean(String.valueOf(aVal));
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
		Map<String,String> mapCol2Json = mapColName2Json.get(aCrudKey);
		for(DBColMeta col : cols.values())
		{
			String sColJsonName = mapCol2Json.get(col.getColname());
			if(sColJsonName!=null && sColJsonName.equalsIgnoreCase(aJsonName))
			{
				return col;
			}
		}
		return null;
	}
	
	private Map<String, DBColMeta> getTableMetaData(String aKey, JdbcDBMgr aDBMgr, String aTableName) throws SQLException
	{
		Map<String, DBColMeta> mapDBColJson = new HashMap<String, DBColMeta>();
		String sSQL = "SELECT * FROM "+aTableName+" WHERE 1=2";

		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		
		try{
			conn = aDBMgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			rs = stmt.executeQuery();
			
			Map<String, String> mapColJsonName = mapColName2Json.get(aKey);
			
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
			aDBMgr.closeQuietly(conn, stmt, rs);
		}
		
		if(mapDBColJson.size()==0)
			return null;
		else
		{
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
		JSONArray jsonArr   = new JSONArray();
		
		if(listParams!=null && listParams.size()>0)
		{
			for(int i=0; i<listParams.size(); i++)
			{
				Object o = listParams.get(i);
				
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
	
	private long updateChildObject(JdbcDBMgr aDBMgr, String aObjInsertSQL, List<Object[]> aListParams) throws Exception
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
			StringBuffer sbObjDel = new StringBuffer();
			sbObjDel.append(" DELETE FROM ").append(sChildTableName);
			sbObjDel.append(" WHERE (").append(sChildTableFields).append(") ");
			sbObjDel.append(" NOT IN ( ");
			sbObjDel.append(" 	SELECT ").append(sChildTableFields);
			sbObjDel.append("  FROM ").append(sChildTableName);
			sbObjDel.append("  WHERE 1=2 ");

			//
			StringBuffer sbObjSQL2 	= new StringBuffer();
			sbObjSQL2.append("SELECT * FROM ").append(sChildTableName).append(" WHERE 1=1 ");
			sbObjSQL2.append(" AND ").append(sbWhere);
			//
			List<Object[]> listParams_new = new ArrayList<Object[]>(); 
			for(Object[] obj2 : aListParams)
			{
				sbObjDel.append(" OR ").append(sbWhere);
				for(Object o : obj2)
				{
					if((o instanceof String) && o.toString().equals(""))
					{
						o = null;
					}
					listFlattenParam.add(o);
				}
				
				if(aDBMgr.getExecuteQueryCount(sbObjSQL2.toString(), obj2)==0)
				{
					listParams_new.add(obj2);
				}
			}
			sbObjDel.append(" ) ");
			aListParams.clear();
			aListParams.addAll(listParams_new);
			listParams_new.clear();
								
			try {
				lAffectedRow += aDBMgr.executeUpdate(sbObjDel.toString(), listFlattenParam);
			}
			catch(SQLException sqlEx)
			{
				throw new Exception("sql:"+aObjInsertSQL+", params:"+listParamsToString(aListParams), sqlEx);
			}
			
			
			
			aObjInsertSQL = aObjInsertSQL.replaceAll("\\{.+?\\}", "?");
			try{
				lAffectedRow += aDBMgr.executeBatchUpdate(aObjInsertSQL, aListParams);
			}
			catch(SQLException sqlEx)
			{
				throw new Exception("sql:"+aObjInsertSQL+", params:"+listParamsToString(aListParams), sqlEx);
			}
		}
		return lAffectedRow;
	}
	
	
	private List<Object[]> getSubQueryParams(Map<String, String> aCrudCfgMap, JSONObject aJsonParentData, String aJsonName) throws Exception
	{
		String sPrefix 		= "jsonattr."+aJsonName+".";
		String sObjSQL 		= aCrudCfgMap.get(sPrefix+JsonCrudConfig._PROP_KEY_OBJ_SQL);
		String sObjMapping 	= aCrudCfgMap.get(sPrefix+JsonCrudConfig._PROP_KEY_OBJ_MAPPING);
		
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
			throw new Exception("No object mapping found ! - "+aJsonName);
		///		
		
		return listAllParams;
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
	
}
