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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import hl.jsoncrud.ValidationMgr.Validation;

public class CRUDMgr {
	
	
    // ANSI escape codes
    public static final String PRN_RESET  = "\u001B[0m";
    public static final String PRN_RED    = "\u001B[31m";
    public static final String PRN_GREEN  = "\u001B[32m";
    public static final String PRN_BLUE   = "\u001B[34m";
	//
	public final static String JSONATTR_ERRCODE				= "error_code";
	public final static String JSONATTR_ERRMSG				= "error_msg";
	//
	public final static String JSONFILTER_IN 				= ".in";
	public final static String JSONFILTER_FROM 				= ".from";
	public final static String JSONFILTER_TO 				= ".to";
	public final static String JSONFILTER_STARTWITH			= ".startwith";
	public final static String JSONFILTER_ENDWITH			= ".endwith";
	public final static String JSONFILTER_CONTAIN			= ".contain";
	public final static String JSONFILTER_CASE_INSENSITIVE	= ".ci";
	public final static String JSONFILTER_NOT				= ".not";

	public final static String JSONFILTER_VALUE_NULL		= "NULL";

	private static List<String> listFilterOperator	= new ArrayList<String>();
	static 
	{
		listFilterOperator.add(CRUDMgr.JSONFILTER_IN);
		listFilterOperator.add(CRUDMgr.JSONFILTER_FROM);
		listFilterOperator.add(CRUDMgr.JSONFILTER_TO);
		listFilterOperator.add(CRUDMgr.JSONFILTER_STARTWITH);
		listFilterOperator.add(CRUDMgr.JSONFILTER_ENDWITH);
		listFilterOperator.add(CRUDMgr.JSONFILTER_CONTAIN);
		listFilterOperator.add(CRUDMgr.JSONFILTER_CASE_INSENSITIVE);
		listFilterOperator.add(CRUDMgr.JSONFILTER_NOT);
	}
	//
	public final static String JSONSORTING_ASC				= "asc";
	public final static String JSONSORTING_DESC				= "desc";
	//
	public final static String JSONVALUE_IN_SEPARATOR 		= ";";
	//
	private final static String SQL_IN_SEPARATOR			= ",";
	private final static String SQLLIKE_WILDCARD			= "%";
	private final static char[] SQLLIKE_RESERVED_CHARS		= new char[]{'%','_'};
	
	public final static String _JSONNAME_TEMPL1 = "[a-zA-Z0-9_][a-zA-Z0-9\\._-]*";
	public final static String _JSON_ATTRNAME 	= _JSONNAME_TEMPL1 + "(?:\\.\\{" +_JSONNAME_TEMPL1+"\\})?";
	
	private boolean isAbsoluteCursorSupported = true;
	
	private final static String REGEX_JSONFILTER = "(?:("+JSONFILTER_NOT+"))?"
			+"(?:("+JSONFILTER_FROM+"|"+JSONFILTER_TO+"|"+JSONFILTER_IN+"|"
			+JSONFILTER_STARTWITH+"|"+JSONFILTER_ENDWITH+"|"+JSONFILTER_CONTAIN+"|"+JSONFILTER_NOT+"|"
			+JSONFILTER_CASE_INSENSITIVE+"))"
		+"(?:("+JSONFILTER_CASE_INSENSITIVE+"|"+JSONFILTER_NOT+"))?"
		+"(?:("+JSONFILTER_CASE_INSENSITIVE+"|"+JSONFILTER_NOT+"))?";
	
	
	private Object initLock = new Object();
	private List<String> listProcessedDynamicSQL				= Collections.synchronizedList(new ArrayList<String>());
	private Map<String, JdbcDBMgr> mapDBMgr 					= null;
	private Map<String, Map<String, String>> mapJson2ColName 	= null;
	private Map<String, Map<String, String>> mapColName2Json 	= null;
	private Map<String, Map<String, String>> mapJson2Sql 		= null;
	private Map<String, Map<String,DBColMeta>> mapTableCols 	= null;
	
	private Map<String, Long> mapLastUpdates 					= new HashMap<String, Long>();
	
	private Pattern pattSQLjsonname		= null;
	private Pattern pattJsonColMapping 	= null;
	private Pattern pattJsonSQL 		= null;
	private Pattern pattJsonFilters		= null;
	private Pattern pattInsertSQLtableFields 	= null;
		
	private JsonCrudConfig jsoncrudConfig 		= null;
	private String config_prop_filename 		= null;
	
	private ValidationMgr validationMgr 		= new ValidationMgr();
	private boolean isAutoValidateRegex			= false;
	
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
		jsonVer.put("version", "0.8.6 beta");
		return jsonVer;
	}
	
	public CRUDMgr(String aPropFileName)
	{
		config_prop_filename = aPropFileName;
		if(aPropFileName!=null && aPropFileName.trim().length()>0)
		{
			try {
				jsoncrudConfig = new JsonCrudConfig(aPropFileName);
			} catch (JsonCrudException e) {
				logger.log(Level.SEVERE, e.getMessage(), e);
				throw new RuntimeException("Error loading "+aPropFileName, e);
			}
		}
		init();
	}
	
	public CRUDMgr(Properties aProp)
	{
		try {
			jsoncrudConfig = new JsonCrudConfig(aProp);
		} catch (JsonCrudException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			throw new RuntimeException("Error loading Properties "+aProp.toString(), e);
		}
		init();
	}

	private synchronized void init()
	{
		logger.log(Level.INFO, "CRUDMgr.init() start.");
		synchronized (initLock) {

			mapLastUpdates		= new HashMap<String, Long>();
			mapDBMgr 			= new HashMap<String, JdbcDBMgr>();
			mapJson2ColName 	= new HashMap<String, Map<String, String>>();
			mapColName2Json 	= new HashMap<String, Map<String, String>>();
			mapJson2Sql 		= new HashMap<String, Map<String, String>>();
			
			mapTableCols		= new HashMap<String, Map<String, DBColMeta>>();
			
			//
			pattJsonColMapping 	= Pattern.compile("jsonattr\\.("+_JSON_ATTRNAME+"?)\\.colname");
			pattJsonSQL 		= Pattern.compile("jsonattr\\.(.+?)\\.sql");
			//
			pattSQLjsonname 			= Pattern.compile("\\{(.+?)\\}");
			pattInsertSQLtableFields 	= Pattern.compile("insert\\s+?into\\s+?("+_JSON_ATTRNAME+"?)\\s+?\\((.+?)\\)");
			//
			pattJsonFilters 	= Pattern.compile(REGEX_JSONFILTER);
			
			try {
				reloadProps();
				
				if(jsoncrudConfig.getAllConfig().size()==0)
				{
					throw new IOException("Fail to load properties file - "+config_prop_filename);
				}
				
				
			} catch (Exception e) {
				logger.log(Level.SEVERE, e.getMessage(), e);
				throw new RuntimeException(e.getMessage(), e);
			}
			
			initPaginationConfig();
			initValidationErrCodeConfig();
			initValidationRuleConfig();
		}
		
		logger.log(Level.INFO, "CRUDMgr.init() completed.");
	}

	private void initValidationRuleConfig()
	{
		Pattern pattValidationRule = Pattern.compile("(.+)?\\.(.+)");
		Map<String, String> mapValidation = jsoncrudConfig.getConfig(JsonCrudConfig._VALIDATION_RULE_CONFIG_KEY);
		if(mapValidation!=null && mapValidation.size()>0)
		{
			Map<String, Map<String, String>> mapValidationRules = new HashMap<String, Map<String, String>>();
			Matcher m = null;
			for(String sKey : mapValidation.keySet())
			{
				m = pattValidationRule.matcher(sKey);
				if(m.find())
				{
					String sRuleName 			= m.group(1);
					String sRuleConfigKey 		= m.group(2);
					String sConfigVal 			= mapValidation.get(sKey);
					
					Map<String, String> mapRuleConfig = mapValidationRules.get(sRuleName.toUpperCase());
					if(mapRuleConfig==null)
					{
						mapRuleConfig = new HashMap<String, String>();
					}
					mapRuleConfig.put(sRuleConfigKey, sConfigVal);
					mapValidationRules.put(sRuleName.toUpperCase(), mapRuleConfig);
				}
			}
			
			//Init Validation Rule
			for(String sRuleName : mapValidationRules.keySet())
			{
				Map<String, String> mapRuleConfig = mapValidationRules.get(sRuleName);
				
				String sRegex 	= mapRuleConfig.get(JsonCrudConfig.VALIDATION_REGEX);
				String sErrCode = mapRuleConfig.get(JsonCrudConfig.VALIDATION_ERRCODE);
				String sErrMsg 	= mapRuleConfig.get(JsonCrudConfig.VALIDATION_ERRMSG);
				
				if(sRegex!=null && (sErrCode!=null || sErrMsg!=null))
				{
					try {
						
						if(validationMgr.addValitionRule(sRuleName, sRegex, sErrCode, sErrMsg)!=null)
						{
							logger.log(Level.INFO, "[ValidationRule] "+sRuleName+" added.");
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
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
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_DBCONNEXCEPTION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_DBCONNEXCEPTION = sMetaKey;		
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_SQLEXCEPTION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_SQLEXCEPTION = sMetaKey;		
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_PLUGINEXCEPTION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_PLUGINEXCEPTION = sMetaKey;		
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_FILTERS);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_FILTERS = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_SORTING);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_SORTING = sMetaKey;
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_PAGINATION);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_PAGINATION = sMetaKey;	
			
			sMetaKey = mapErrCodes.get(JsonCrudConfig.ERRCODE_INVALID_RETURNS);
			if(sMetaKey!=null)
				JsonCrudConfig.ERRCODE_INVALID_RETURNS = sMetaKey;	
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
	
	public void setAutoValidateRegex(boolean aIsAutoValidateRegex)
	{
		this.isAutoValidateRegex = aIsAutoValidateRegex;
	}
	
	public JSONArray validateJSONDataWithRegex(String aCrudKey, JSONObject aDataJson) throws JsonCrudException
	{
		Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(aCrudKey);
		if(mapCrudConfig==null)
			mapCrudConfig = new HashMap<String, String>();
		
		JSONArray jArrErrors = new JSONArray();
		for(String sJsonAttr : aDataJson.keySet())
		{
			String sJsonVal = String.valueOf(aDataJson.get(sJsonAttr));
			
			String sValidateCfgKey 		= "jsonattr."+sJsonAttr+".validation.rule";
			
			if(sValidateCfgKey!=null && sValidateCfgKey.trim().length()>0)
			{
				String sValidateRuleNames 	= mapCrudConfig.get(sValidateCfgKey);
				
				if(sValidateRuleNames==null)
					sValidateRuleNames = "";
				
				StringTokenizer tk = new StringTokenizer(sValidateRuleNames, ",");
				while(tk.hasMoreTokens())
				{
					String sRuleName = tk.nextToken(); 
					if(sRuleName!=null && sRuleName.trim().length()>0)
					{
						Validation v = validationMgr.validate(sRuleName.trim(), sJsonVal);
						if(v!=null && !v.isValidated_ok())
						{
							StringBuffer sbErr = new StringBuffer();
							if(v.getErr_code()!=null)
							{
								sbErr.append(v.getErr_code());
							}
							
							if(v.getErr_msg()!=null)
							{
								if(sbErr.length()>0)
								{
									sbErr.append(" - ");
								}
								sbErr.append(v.getErr_msg());
							}
							
							JSONObject jsonErr = new JSONObject();
							jsonErr.put(sJsonAttr, sbErr.toString());
							jArrErrors.put(jsonErr);
						}
						else if(v==null)
						{
							throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Missing 'validation.rule' configuration ! "+sRuleName);
						}
					}
				}
			}
		}
		
		return jArrErrors;
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
		
		Map<String, String> mapCrudJsonSql 		= mapJson2Sql.get(aCrudKey);
		Map<String, String> mapCrudJsonCol 		= mapJson2ColName.get(aCrudKey);
		Map<String, DBColMeta> mapCrudDBCols 	= mapTableCols.get(aCrudKey);
		
		if(mapCrudJsonCol==null)
			return aDataJson;
		
		if(mapCrudDBCols==null)
			mapCrudDBCols = new HashMap<String, DBColMeta>();
		
		for(String sJsonAttr : aDataJson.keySet())
		{
			boolean isInJsonCrudConfig 	= mapCrudJsonCol.get(sJsonAttr)!=null;
			boolean isInDBSchema 		= mapCrudDBCols.get(sJsonAttr)!=null;
			boolean isInJsonSQL			= mapCrudJsonSql.get(sJsonAttr)!=null;
			if(!isInJsonCrudConfig && !isInDBSchema && !isInJsonSQL)
			{
				if(isExceptionOnUnknownJsonAttr)
				{
					JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid input data !");
					e.setErrorSubject(sJsonAttr);
					e.setErrorDebugInfo("CrudKey:["+aCrudKey+"], Key:["+sJsonAttr+"]");	
					throw e;
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
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key !");
			e.setErrorSubject(aCrudKey);
			throw e;
		}
		
		if ("true".equalsIgnoreCase(mapCrudCfg.get(JsonCrudConfig._PROP_KEY_RETRIEVEONLY)))
		{
			return null;
		}
		
		if(isAutoValidateRegex)
		{
			JSONArray jArrErrors = validateJSONDataWithRegex(aCrudKey, aDataJson);
			if(jArrErrors!=null && jArrErrors.length()>0)
			{
				//TODO Throw 1 Error first
				//System.out.println("[create.validation]"+jArrErrors.toString());
				for(int i=0; i<jArrErrors.length(); i++)
				{
					JSONObject jsonErr = jArrErrors.getJSONObject(i);
					throw new JsonCrudException(jsonErr.getString(JSONATTR_ERRCODE), jsonErr.getString(JSONATTR_ERRMSG));
				}
			}
		}
		
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
		if(mapCrudJsonCol==null)
			mapCrudJsonCol = new HashMap<String, String>();
		
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
		
		if(sTableName!=null)
		{
		
			String sSQL 		= "INSERT INTO "+sTableName+"("+sbColName.toString()+") values ("+sbParams.toString()+")";
			
			String sJdbcName 	= mapCrudCfg.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
			JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
			
			JSONArray jArrCreated = null; 
			
			try {
				jArrCreated = dbmgr.executeUpdate(sSQL, listValues);
			}
			catch(Throwable ex)
			{
				String sDebugMsg = "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(listValues);
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, ex);
				e.setErrorDebugInfo(sDebugMsg);
				throw e;
			}
		
			if(jArrCreated.length()>0)
			{
				JSONObject jsonCreated = jArrCreated.getJSONObject(0);
				jsonCreated = convertCol2Json(aCrudKey, jsonCreated, false);
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
							List<Object[]> listParams2 	= getInsertSubQueryParams(mapCrudCfg, jsonReturn, sJsonName2);
							String sObjInsertSQL 		= mapCrudCfg.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_CHILD_INSERTSQL);
							
							long lupdatedRow 	= 0;
							
							try {
								lupdatedRow 	= updateChildObject(dbmgr, sObjInsertSQL, listParams2);
							}
							catch(Throwable ex)
							{
								String sDebugMsg = null;							
								try {
									//rollback parent
									sbRollbackParentSQL.insert(0, "DELETE FROM "+sTableName+" WHERE 1=1 ");
	
									JSONArray jArrRollbackRows = dbmgr.executeUpdate(sbRollbackParentSQL.toString(), listValues);
									if(jArrCreated.length() != jArrRollbackRows.length())
									{
										JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, "Record fail to Rollback!");
										e.setErrorDebugInfo("[Rollback Failed] sql:"+sbRollbackParentSQL.toString()+",params:"+listParamsToString(listValues));
										throw e;
									}
								}
								catch(Throwable ex2)
								{
									sDebugMsg = "[Rollback Failed] parent - [sql:"+sbRollbackParentSQL.toString()+",params:"+listParamsToString(listValues)+"], child:[sql:"+sObjInsertSQL+",params:"+listParamsToString(listParams2)+"]";
									ex = ex2;
								}
								
								if(sDebugMsg==null)
								{
									sDebugMsg = "[Rollback Success] child - sql:"+sObjInsertSQL+", params:"+listParamsToString(listParams2);
								}
								
								JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, ex);
								e.setErrorDebugInfo(sDebugMsg);
								throw e;
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
		}
		return null;
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
		JSONObject json = retrieve(aCrudKey, aWhereJson, 0, 0, null, null, false);
		if(json==null)
		{
			return new JSONArray();
		}
		return (JSONArray) json.get(JsonCrudConfig._LIST_RESULT);
	}
	
	public JSONArray retrieve(String aCrudKey, JSONObject aWhereJson, String[] aSorting, String[] aReturns) throws JsonCrudException 
	{
		return retrieve(aCrudKey, aWhereJson, aSorting, aReturns, false);
	}
	
	public JSONArray retrieve(String aCrudKey, JSONObject aWhereJson, String[] aSorting, String[] aReturns, boolean isReturnsExcludes) 
			throws JsonCrudException
	{
		JSONObject json = retrieve(aCrudKey, aWhereJson, 0, 0, aSorting, aReturns, isReturnsExcludes);
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
			
			if(lAffectRows>0)
				updLastUpdatedTimestamp(aCrudKey, System.currentTimeMillis());
		}
		catch(SQLException sqlEx)
		{
			if(conn==null)
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_DBCONNEXCEPTION, sqlEx);
				throw e;
			}
			
			String sDebugMsg = "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams);
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
			e.setErrorDebugInfo(sDebugMsg);
			throw e;		
		}
		finally
		{
			if(dbmgr!=null)
				dbmgr.closeQuietly(conn, stmt, null);
		}
		
		return lAffectRows;
	}
	
	private List<String> getSQLJsonAttrs(String aCrudKey, String aSQL, Object[] aObjParams) throws JsonCrudException
	{
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null)
			return null;
		
		List<String> listfields = new ArrayList<String>();

		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		String sSQL = aSQL;
		
		Map<String, String> mapCrudColJson = mapColName2Json.get(aCrudKey);
		if(mapCrudColJson==null)
			mapCrudColJson = new HashMap<String,String>();
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		try {
			conn = dbmgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			stmt = JdbcDBMgr.setParams(stmt, aObjParams);
			
			conn.setReadOnly(true);
			stmt.setFetchSize(10);
			rs = stmt.executeQuery();
			ResultSetMetaData meta = rs.getMetaData();
			
			for(int i=1; i<=meta.getColumnCount();i++)
			{
				String sColName = meta.getColumnLabel(i);
				String sJsonName = mapCrudColJson.get(sColName);
				if(sJsonName==null)
					sJsonName = sColName;
				//
				listfields.add(sJsonName);
			}
			
		} 
		catch (SQLException sqlEx) 
		{
			if(conn==null)
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_DBCONNEXCEPTION, sqlEx);
				throw e;
			}
			
			String sDebugMsg = "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams);
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
			e.setErrorDebugInfo(sDebugMsg);
			throw e;
		}
		finally
		{
			try {
				conn.setReadOnly(false);
				dbmgr.closeQuietly(conn, stmt, rs);
			} catch (SQLException e) {
				//ignore
				logger.log(Level.WARNING, e.getMessage(), e);
			}
		}
		return listfields;
	}
	
	private long getTotalSQLCount(String aCrudKey, String aSQL, Object[] aObjParams) throws JsonCrudException
	{
		Map<String, String> map 		= jsoncrudConfig.getConfig(aCrudKey);
		if(map==null)
			return 0;
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		long lTotalCount = 0;
		String sSQL = aSQL;
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		try {
			conn = dbmgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			stmt = JdbcDBMgr.setParams(stmt, aObjParams);
			
			conn.setReadOnly(true);
			stmt.setFetchSize(10);
			rs = stmt.executeQuery();
			if(rs.next())
			{
				lTotalCount = rs.getLong(1);
			}
		} 
		catch (SQLException sqlEx) 
		{
			if(conn==null)
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_DBCONNEXCEPTION, sqlEx);
				throw e;
			}
			
			String sDebugMsg = "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams);
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
			e.setErrorDebugInfo(sDebugMsg);
			
			System.err.println(sDebugMsg);
			throw e;
		}
		finally
		{
			try {
				if(conn!=null)
				{
					conn.setReadOnly(false);
				}
				dbmgr.closeQuietly(conn, stmt, rs);
			} catch (SQLException e) {
				//ignore
				logger.log(Level.WARNING, e.getMessage(), e);
			}
		}
		return lTotalCount;
	}
	
	private JSONObject retrieveBySQL(String aCrudKey, String aSQL, Object[] aObjParams,
			long aStartFrom, long aFetchSize, String[] aReturns) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, aSQL, aObjParams, aStartFrom, aFetchSize, aReturns, false, -1);
	}
	
	private JSONObject retrieveBySQL(String aCrudKey, String aSQL, Object[] aObjParams,
			long aStartFrom, long aFetchSize, String[] aReturns, boolean isReturnsExcludes,
			long aTotalRecordCount) throws JsonCrudException
	{
		JSONObject jsonReturn 			= null;
		Map<String, String> map 		= jsoncrudConfig.getConfig(aCrudKey);
		
		if(map==null || map.size()==0)
		{
			String sErr = "Invalid crudkey - "+aCrudKey;
			logger.log(Level.SEVERE, sErr);
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, sErr);
		}
		
		String sExcludeNonMappedFields 	= map.get(JsonCrudConfig._PROP_KEY_EXCLUDE_NON_MAPPED_FIELDS);
		boolean isExcludeNonMappedField = "true".equalsIgnoreCase(sExcludeNonMappedFields);

		Map<String, String> mapCrudSql 	= mapJson2Sql.get(aCrudKey);
		
		String sSQL 		= aSQL;
		
		String sJdbcName 	= map.get(JsonCrudConfig._PROP_KEY_DBCONFIG);
		JdbcDBMgr dbmgr 	= mapDBMgr.get(sJdbcName);
		
		if(dbmgr==null)
		{
			String sErr = "Invalid configuration - "+aCrudKey+".dbconfig="+sJdbcName;
			logger.log(Level.SEVERE, sErr);
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, sErr);
		}
		
		List<String> listReturnsAttrName = new ArrayList<String>();
		
		if(aReturns!=null)
		{
			for(String sAttrName : aReturns)
			{
				listReturnsAttrName.add(sAttrName);
			}
		}
		
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
			
			try {
				conn.setReadOnly(true);
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				rs   = stmt.executeQuery();
			}
			catch(SQLException sqlEx)
			{
				StringBuffer sbErr = new StringBuffer();
				sbErr.append(sqlEx.getMessage()).append("\n");
				sbErr.append("sql:").append(sSQL).append("\n");
				sbErr.append("param:");				
				if(aObjParams!=null && aObjParams.length>0)
				{
					for(int i=0; i<aObjParams.length; i++)
					{
						if(i>0)
							sbErr.append(",");
						sbErr.append(String.valueOf(aObjParams[i]));
					}
				}
				sbErr.append("\n");
				throw new SQLException(sbErr.toString(),sqlEx);
			}
			
			long lTotalResult 		= 0;
			ResultSetMetaData meta 	= rs.getMetaData();
			
			if(isAbsoluteCursorSupported && aStartFrom>2)
			{
				try {
					int iCurPos = (int)aStartFrom-1;
					if(rs.absolute(iCurPos))
					{
						lTotalResult = iCurPos;
					}
				}catch(Exception ex)
				{
					//ignore as cursor move is not supported
					isAbsoluteCursorSupported = false;
					logger.fine("[WARNING]"+ex.getMessage());
				}
			}
			
			while(rs.next())
			{	
				lTotalResult++;
				
				if(lTotalResult < aStartFrom)
					continue;

				JSONObject jsonObj = new JSONObject();
				for(int i=0; i<meta.getColumnCount(); i++)
				{
					// need to have full result so that subquery can be execute
					String sColName = meta.getColumnLabel(i+1);
					Object oObj = rs.getObject(sColName);
					if(oObj==null)
						oObj = JSONObject.NULL;
					jsonObj.put(sColName, oObj);
				}
				jsonObj = convertCol2Json(aCrudKey, jsonObj, isExcludeNonMappedField);

				if(mapCrudSql.size()>0)
				{
					for(String sJsonName : mapCrudSql.keySet())
					{
						
						if(isFilterByReturns)
						{
							boolean isReturnsContain = listReturnsAttrName.contains(sJsonName);							

							if(isReturnsContain)
							{
								//is exclude list
								if(isReturnsExcludes)
								{
									continue; //skip to exclude
								}
							}
							else 
							{
								//include list
								if(!isReturnsExcludes)
								{
									continue; //skip as not include
								}
							}
						}
						
						if(!jsonObj.has(sJsonName))
						{
							List<Object> listParams2 = new ArrayList<Object>();
							String sSQL2 = mapCrudSql.get(sJsonName);
							Matcher m = pattSQLjsonname.matcher(sSQL2);
							while(m.find())
							{
								String sBracketJsonName = m.group(1);
								if(jsonObj.has(sBracketJsonName))
								{
									listParams2.add(jsonObj.get(sBracketJsonName));
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
									
									if(sChildMapping.startsWith("[") && sChildMapping.endsWith("]"))
									{
										// mapping=["cfg_key"]
										
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
													JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid Subquery Mapping !");
													e.setErrorSubject(sPropKeyMapping+"."+sChildMapping);
													throw e;
												}
											}
										}
										jsonObj.put(sJsonName, jArrMappingData);
									}
									else if(sChildMapping.startsWith("{") && sChildMapping.endsWith("}"))
									{
										// mapping={"cfg_key":"cfg_value"}
										
										JSONObject jsonMappingData = null;
										JSONObject jsonChildMapping = new JSONObject(sChildMapping);
										int iKeys = jsonChildMapping.keySet().size();
										if(iKeys>0)
										{
											boolean isKVmapping = (iKeys==1);
											
											jsonMappingData = new JSONObject();
											for(int i=0; i<jsonArrayChild.length(); i++)
											{		
												JSONObject jsonData = jsonArrayChild.getJSONObject(i);
												for(String sKey : jsonChildMapping.keySet())
												{
													String sMapKey = null;
													Object oMapVal = null;
													if(isKVmapping)
													{
														sMapKey = jsonData.getString(sKey);
														oMapVal = jsonData.get(jsonChildMapping.getString(sKey));
													}
													else
													{
														sMapKey = sKey;
														oMapVal = jsonData.get(sKey);
													}
													
													jsonMappingData.put(sMapKey, oMapVal);
													
												}
											}
										}
										
										if(jsonMappingData!=null)
										{
											jsonObj.put(sJsonName, jsonMappingData);
										}
										else
										{
											JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid Child Mapping !");
											e.setErrorSubject(sPropKeyMapping+"."+sChildMapping);
											throw e;
										}
									}
									else
									{
										if(jsonArrayChild.length()>0)
										{
											Object oData = jsonArrayChild.get(0);
											if(oData!=null)
											{
												boolean isForceString = 
														sChildMapping.startsWith("\"") && sChildMapping.endsWith("\"");
												
												String sAttrkey = sChildMapping;
												
												if(isForceString)
													sAttrkey = sChildMapping.substring(1, sChildMapping.length()-1);
												
												if(oData instanceof JSONObject)
												{
													JSONObject jsonData = ((JSONObject)oData);
													if(jsonData.has(sAttrkey))
													{
														oData = jsonData.get(sAttrkey);
													}
												}
													
												if(isForceString)
												{
													oData = String.valueOf(oData);
												}
												
												jsonObj.put(sJsonName, oData);
											}
										}
									}
									
								}
								else
								{
									jsonObj.put(sJsonName, jsonArrayChild);			
								}
							}
						}
					}
				}
				
				
				if(isFilterByReturns)
				{
					JSONObject jsonObjReturn = new JSONObject();
					for(Object oAttrKey : jsonObj.keySet())
					{
						String sAttKey = oAttrKey.toString();
						
						boolean isReturnsContain = listReturnsAttrName.contains(sAttKey);
						
						if(isReturnsContain)
						{
							//exclude list
							if(isReturnsExcludes)
							{
								continue;
							}
						}
						else 
						{
							//include list
							if(!isReturnsExcludes)
							{
								continue;
							}
						}
						
						jsonObjReturn.put(sAttKey, jsonObj.get(sAttKey));
					}
					jsonObj = jsonObjReturn;
					
				}
				
				jsonArr.put(jsonObj);
				
				if(aFetchSize>0 && jsonArr.length()>=aFetchSize)
				{
					break;
				}
			}
			
			long lTotalCount = lTotalResult;
			
			
			if(aTotalRecordCount<0)
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
			if(conn==null)
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_DBCONNEXCEPTION, sqlEx);
				throw e;
			}
			
			String sDebugMsg = "crudKey:"+aCrudKey+", sql:"+sSQL+", params:"+listParamsToString(aObjParams);
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
			e.setErrorDebugInfo(sDebugMsg);
			throw e;
		}
		finally
		{
			if(conn!=null)
			{
				//
				try {
					conn.setReadOnly(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			dbmgr.closeQuietly(conn, stmt, rs);
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
		return retrieveBySQL(aCrudKey, null, aWhereJson, aStartFrom, aFetchSize, aSorting, aReturns, false);
	}
	
	public JSONObject retrieve(String aCrudKey, JSONObject aWhereJson, 
			long aStartFrom, long aFetchSize, String[] aSorting, String[] aReturns, boolean isReturnsExcludes) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, null, aWhereJson, aStartFrom, aFetchSize, aSorting, aReturns, isReturnsExcludes);
	}

	
	public JSONObject retrieveBySQL(String aCrudKey, 
			String aTableViewSQL,
			JSONObject aWhereJson, 
			long aStartFrom, long aFetchSize, 
			String[] aSorting, String[] aReturns) throws JsonCrudException
	{
		return retrieveBySQL(aCrudKey, aTableViewSQL, aWhereJson, aStartFrom, aFetchSize, aSorting, aReturns, false);
	}

	public JSONObject retrieveBySQL(String aCrudKey, 
			String aTableViewSQL,
			JSONObject aWhereJson, 
			long aStartFrom, long aFetchSize, 
			String[] aSorting, String[] aReturns, boolean isReturnsExcludes) throws JsonCrudException
	{
		
		Map<String, String> map = jsoncrudConfig.getConfig(aCrudKey);
		if(map==null || map.size()==0)
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key !");
			e.setErrorSubject(aCrudKey);
			throw e;
		}

		try {
			
			if(aTableViewSQL==null && "true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_RETRIEVEONLY)))
			{
				aTableViewSQL = map.getOrDefault(JsonCrudConfig._PROP_KEY_SQL, null);
			}
			
			if(aTableViewSQL!=null)
			{
				if(!listProcessedDynamicSQL.contains(aTableViewSQL))
				{
					synchronized(listProcessedDynamicSQL)
					{
						if(!listProcessedDynamicSQL.contains(aTableViewSQL))
						{
							listProcessedDynamicSQL.add(aTableViewSQL);
							
							synchronized(initLock)
							{
								Map<String, DBColMeta> mapNewCols 		= getTableMetaDataBySQL(aCrudKey, aTableViewSQL);
								Map<String, DBColMeta> mapExistingCols 	= mapTableCols.get(aCrudKey);
								
								if(mapNewCols==null)
									mapNewCols = new HashMap<String, DBColMeta>();
								
								if(mapExistingCols==null)
									mapExistingCols = new HashMap<String, DBColMeta>();
								
								for(String sColName : mapNewCols.keySet())
								{
									if(mapExistingCols.get(sColName)==null)
									{
										//logger.log(Level.FINEST, "[+]"+sColName+":"+mapNewCols.get(sColName));
										mapExistingCols.put(sColName, mapNewCols.get(sColName));
									}
								}
								mapTableCols.put(aCrudKey, mapExistingCols);
							}
						}		
					}
				}
			}
			
		} catch (JsonCrudException e) {
			// Silent, as this is just a pre-process
			logger.log(Level.WARNING, e.getMessage(), e);
		}
		
		if(aReturns==null)
			aReturns = new String[]{};
		
		JSONObject jsonWhere = castJson2DBVal(aCrudKey, aWhereJson);
		if(jsonWhere==null)
			jsonWhere = new JSONObject();
		
		List<Object> listValues 			= new ArrayList<Object>();		
		Map<String, String> mapCrudJsonCol 	= mapJson2ColName.get(aCrudKey);

		String sTableName 	= map.get(JsonCrudConfig._PROP_KEY_TABLENAME);

		if((aTableViewSQL==null) && (sTableName==null || sTableName.trim().length()==0))
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
			Object oJsonValue 	= jsonWhere.get(sOrgJsonName);
			Map<String, String> mapSQLEscape = new HashMap<String, String>();
			
			String sJsonName 	= removeJsonNameFilters(sOrgJsonName);	
			String sColName = mapCrudJsonCol.get(sJsonName);
			
			if(sColName!=null && sOrgJsonName.length()!=sJsonName.length())
			{
				String sFilters = sOrgJsonName.substring(sJsonName.length());
				
				Matcher m = pattJsonFilters.matcher(sFilters);
				if(m.find())
				{
					String sJsonNOT 		= m.group(1);
					String sJsonOperator 	= m.group(2);
					String sJsonCIorNOT_1 	= m.group(3);
					String sJsonCIorNOT_2 	= m.group(4);
										
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
						
						if(JSONFILTER_VALUE_NULL.equalsIgnoreCase(sJsonValue))
						{
							oJsonValue = JSONObject.NULL;
						}
						else if(JSONFILTER_STARTWITH.equals(sJsonOperator))
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
					sColName = mapCrudJsonCol.get(sJsonName);
				}
				else
				{
					sColName = mapCrudJsonCol.get(sJsonName);
					
					if(sColName==null)
					{
						int iPos = sJsonName.indexOf(".");
						if(iPos>-1)
						{
							String sFilterOps = sJsonName.substring(iPos);
							if(sFilterOps.length()>0)
							{
								//invalid operators
								JsonCrudException e = new JsonCrudException(
										JsonCrudConfig.ERRCODE_INVALID_FILTERS, "Invalid filters operator ! - "+sJsonName);
								e.setErrorSubject(sJsonName);
								throw e;
							}
						}
					}
				}
			}
			
			boolean isUseSQLANY = true;
			//
			if(sColName!=null)
			{
				sbWhere.append(" AND ");
				
				if(oJsonValue==null || oJsonValue==JSONObject.NULL)
				{
					sbWhere.append(sColName).append(" IS");
					if(isNotCondition)
					{
						sbWhere.append(" NOT");
					}
					sbWhere.append(" NULL");
					continue;
				}
				
				if(isNotCondition)
				{
					sbWhere.append(" NOT (");
				}
				
				String sCIPrefix 	= "";
				String sCIPostfix 	= "";
				if(isCaseInSensitive && (oJsonValue instanceof String))
				{
					sCIPrefix 	= " UPPER(";
					sCIPostfix = ") ";
				}

				StringBuffer sbSQLparam = new StringBuffer();
				sbSQLparam.append(sCIPrefix).append("?").append(sCIPostfix);
				
				boolean isINCondition = sOperator.trim().equalsIgnoreCase("IN");
				if(isINCondition)
				{
					//multi-value
					sbSQLparam.setLength(0);
					String sStrValues = oJsonValue.toString();
					
					String sValSeparator = SQL_IN_SEPARATOR;
					if(sStrValues.indexOf(JSONVALUE_IN_SEPARATOR)>-1)
					{
						sValSeparator = JSONVALUE_IN_SEPARATOR;
					}
					StringTokenizer tk = new StringTokenizer(sStrValues, sValSeparator);
					List<Object> listAnyObj = new ArrayList<Object>();
					
					while(tk.hasMoreTokens())
					{
						String sVal = tk.nextToken();
						oJsonValue = castJson2DBVal(aCrudKey, sJsonName, sVal.trim());
						
						if(oJsonValue!=JSONObject.NULL)
						{	
							if(isUseSQLANY)
							{
								listAnyObj.add(oJsonValue);
							}
							else
							{
								if(sbSQLparam.length()>0)
									sbSQLparam.append(", ");
								
								sbSQLparam.append(sCIPrefix).append("?").append(sCIPostfix);
								listValues.add(oJsonValue);
								
							}
						}
					}
					
					if(isUseSQLANY)
					{
						if(oJsonValue instanceof String)
						{
							listValues.add( (String[]) listAnyObj.toArray(new String[listAnyObj.size()]));
						}
						else if(oJsonValue instanceof Long)
						{
							listValues.add( (Long[]) listAnyObj.toArray(new Long[listAnyObj.size()]));
						}
						else if(oJsonValue instanceof Double)
						{
							listValues.add( (Double[]) listAnyObj.toArray(new Double[listAnyObj.size()]));
						}
						else if(oJsonValue instanceof Boolean)
						{
							listValues.add( (Boolean[]) listAnyObj.toArray(new Boolean[listAnyObj.size()]));
						}
						else 
						{
							listValues.add(listAnyObj.toArray());
						}
					}
					
					
				}
				else
				{
					oJsonValue = castJson2DBVal(aCrudKey, sJsonName, oJsonValue);
					listValues.add(oJsonValue);
				}
				
				String sINPrefix 	= "";
				String sINPostfix 	= "";
				
				if(isINCondition)
				{
					sINPrefix = " (";
					sINPostfix = ") ";
					
					if(isUseSQLANY)
					{
						sOperator = " = ANY ";
						
						sbSQLparam.setLength(0);
						sbSQLparam.append("?");
					}
				}
				
				sbWhere.append(sCIPrefix).append(sColName).append(sCIPostfix).append(sOperator);
				sbWhere.append(sINPrefix).append(sbSQLparam.toString()).append(sINPostfix);
				
				
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
				{
					JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_FILTERS, 
							"Invalid filters attribute !");
					e.setErrorSubject(sJsonName);
					throw e;
				}
			}
		}
		
		List<String> listSelectFields = new ArrayList<String>();
		
		StringBuffer sbOrderBy = new StringBuffer();
		if(aSorting!=null && aSorting.length>0)
		{
			for(String sOrderBy : aSorting)
			{
				String sOrderSeqKeyword = "";
				int iOrderSeq = sOrderBy.lastIndexOf('.');
				if(iOrderSeq>-1)
				{
					sOrderSeqKeyword = sOrderBy.substring(iOrderSeq+1, sOrderBy.length());
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
					JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_SORTING, 
							"Invalid sorting attribute !");
					e.setErrorSubject(sJsonAttr);
					throw e;
				}
			}
		}
		
		if(aTableViewSQL!=null && aTableViewSQL.length()>0)
		{
			sTableName = "("+aTableViewSQL+") AS TBL ";
		}
		
		StringBuffer sbSelectFields = new StringBuffer();
		boolean isTableViewSQL = (aTableViewSQL==null || aTableViewSQL.length()==0);
		
		if(aReturns.length>0 && !isReturnsExcludes)
		{
			
			for(String sReturn : aReturns)
			{
				String sDBColName = mapCrudJsonCol.get(sReturn);
				if(sDBColName==null)
				{
					sDBColName = sReturn;
				}
				else if (isTableViewSQL)
				{
					//DB column
					if(sbSelectFields.length()>0)
						sbSelectFields.append(", ");
					sbSelectFields.append(sDBColName);
				}
				
				if(!listSelectFields.contains(sDBColName.toUpperCase()))
				{
					listSelectFields.add(sDBColName.toUpperCase());
				}
			}
		}
		
		if(sbSelectFields.length()==0)
		{
			sbSelectFields.append("*");
		}
		
		Object[] objParams = listValues.toArray(new Object[listValues.size()]);
		StringBuffer sbSQL = new StringBuffer();
		sbSQL.append("SELECT COUNT(*) FROM ").append(sTableName).append(" WHERE 100=100 ").append(sbWhere.toString());
		
		long lTotalRecordCount = getTotalSQLCount(aCrudKey, sbSQL.toString(), objParams);
		
		sbSQL.setLength(0);
		sbSQL.append(" SELECT ").append(sbSelectFields.toString()).append(" FROM ").append(sTableName);
		sbSQL.append(" WHERE 101=101 ").append(sbWhere.toString());
		if(sbOrderBy.length()>0)
		{
			sbSQL.append(" ORDER BY ").append(sbOrderBy.toString());
		}
		
		JSONObject jsonReturn = null;
		try {

			logger.log(Level.FINE, sbSQL.toString());
			
			jsonReturn 	= retrieveBySQL(
					aCrudKey, sbSQL.toString(), 
					objParams, 
					aStartFrom, aFetchSize, 
					aReturns, isReturnsExcludes, 
					lTotalRecordCount);
		}
		catch(JsonCrudException jsonEx)
		{
			if(jsonEx.getThrowable() instanceof SQLException )
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, jsonEx);
				
				if(aReturns.length>0 )
				{
					if(logger.isLoggable(Level.FINER)) //trace level
					{
						sbSQL.setLength(0);
						sbSQL.append(" SELECT * FROM ").append(sTableName).append(" WHERE 101=102 ").append(sbWhere.toString());

						List<String> listJsonAttrs = getSQLJsonAttrs(aCrudKey, sbSQL.toString(), objParams);
						if(listJsonAttrs==null)
							listJsonAttrs = new ArrayList<String>();
						
						for(int i=0; i<aReturns.length; i++)
						{
							String sReturnAttrName = aReturns[i];
							if(!listJsonAttrs.contains(sReturnAttrName))
							{
								e = new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_RETURNS, 
										"Invalid returns attribute !");
								e.setErrorSubject(sReturnAttrName);
								throw e;
							}
						}
					}
					throw e;
				}
			}
			throw jsonEx;
		}
		
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
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key !");
			e.setErrorSubject(aCrudKey);
			throw e;
		}
		
		if ("true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_RETRIEVEONLY)))
		{
			return null;
		}

		if(isAutoValidateRegex)
		{
			JSONArray jArrErrors = validateJSONDataWithRegex(aCrudKey, aDataJson);
			if(jArrErrors!=null && jArrErrors.length()>0)
			{
				//TODO Throw 1 Error first
				//System.out.println("[update.validation]"+jArrErrors.toString());
				StringBuffer sbErrCode 	= new StringBuffer();
				StringBuffer sbErrMsg 	= new StringBuffer();
				
				for(int i=0; i<jArrErrors.length(); i++)
				{
					JSONObject jsonErr = jArrErrors.getJSONObject(i);
					sbErrCode.append("ErrCode:").append(jsonErr.getString(JSONATTR_ERRCODE));
					sbErrCode.append("\n");
					
					sbErrMsg.append("[").append(jsonErr.getString(JSONATTR_ERRCODE));
					sbErrMsg.append("] ErrMsg:").append(jsonErr.getString(JSONATTR_ERRMSG));
					sbErrMsg.append("\n");
				}
				
				throw new JsonCrudException(sbErrCode.toString(), sbErrMsg.toString());
			}
		}
		
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
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, 
						"Missing Json to dbcol mapping !");
				e.setErrorSubject(aCrudKey+"."+sJsonName);
				e.setErrorDebugInfo(sJsonName+":"+jsonWhere.get(sJsonName));
				throw e;
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
				if(jArrUpdated.length()>0)
					updLastUpdatedTimestamp(aCrudKey, System.currentTimeMillis());
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
						List<Object[]> listParams2 	= getInsertSubQueryParams(map, jsonReturn, sJsonName2);
						String sObjInsertSQL 		= map.get("jsonattr."+sJsonName2+"."+JsonCrudConfig._PROP_KEY_CHILD_INSERTSQL);
						
						lAffectedRow2 += updateChildObject(dbmgr, sObjInsertSQL, listParams2);
					}
				}
				
				if(lAffectedRow2>0)
					updLastUpdatedTimestamp(aCrudKey, System.currentTimeMillis());
			}
		}
		catch(SQLException sqlEx) 
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
			e.setErrorDebugInfo("sql:"+sSQL+", params:"+listParamsToString(listValues));
			throw e;
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
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid crud configuration key !");
			e.setErrorSubject(aCrudKey);
			throw e;
		}
		
		if ("true".equalsIgnoreCase(map.get(JsonCrudConfig._PROP_KEY_RETRIEVEONLY)))
		{
			return null;
		}
		
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
			updLastUpdatedTimestamp(aCrudKey, System.currentTimeMillis());
			
			JSONArray jArrAffectedRow = new JSONArray();
			try {
				jArrAffectedRow = dbmgr.executeUpdate(sSQL, listValues);
			}
			catch(SQLException sqlEx)
			{
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
				e.setErrorDebugInfo("sql:"+sSQL+", params:"+listParamsToString(listValues));
				throw e;
			}
			
			if(jArrAffectedRow.length()>0)
			{
				updLastUpdatedTimestamp(aCrudKey, System.currentTimeMillis());
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
	
	private JdbcDBMgr initNRegJdbcDBMgr(String aJdbcConfigKey, Map<String, String> mapJdbcConfig) throws JsonCrudException
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
					
					
				}
				catch(Throwable ex)
				{
					throw new JsonCrudException("Error initialize JDBC - "+aJdbcConfigKey, ex);
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
	
	public void reloadProps() throws JsonCrudException
	{
		clearAll();
		
		if(jsoncrudConfig==null)
		{
			jsoncrudConfig 	= new JsonCrudConfig(config_prop_filename);
		}
		
		for(String sKey : jsoncrudConfig.getConfigCrudKeys())
		{
			synchronized(mapTableCols)
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
				String sDBTableName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_TABLENAME);
				//
				if(sDBConfigName!=null && sDBConfigName.trim().length()>0)
				{
					dbmgr = mapDBMgr.get(sDBConfigName);
					if(dbmgr==null)
					{
						Map<String, String> mapDBConfig = jsoncrudConfig.getConfig(sDBConfigName);
						
						if(mapDBConfig==null)
						{
							JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid "+JsonCrudConfig._PROP_KEY_DBCONFIG);
							e.setErrorSubject(sDBConfigName);
							throw e;
						}
						
						String sJdbcClassname = mapDBConfig.get(JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME);
						
						if(sJdbcClassname!=null)
						{
							dbmgr = initNRegJdbcDBMgr(sDBConfigName, mapDBConfig);
						}
						else if(sDBTableName!=null && sDBTableName.trim().length()>0)
						{
							JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "Invalid "+JsonCrudConfig._PROP_KEY_JDBC_CLASSNAME+ " - "+sKey+".dbconfig="+sDBConfigName);
							e.setErrorSubject(sJdbcClassname);
							throw e;
						}
					}
				}
				
				if(dbmgr!=null)
				{
					Map<String, String> mapCrudJson2Col 		= new HashMap<String, String> ();
					Map<String, String> mapCrudCol2Json 		= new HashMap<String, String> ();
					Map<String, String> mapCrudJsonSelectSql	= new HashMap<String, String> ();
					
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
								if(sql.trim().toLowerCase().startsWith("select"))
								{
									mapCrudJsonSelectSql.put(jsonname, sql);
								}
								else
								{
									System.err.println("[Non-Select-SQL] "+jsonname+" sql="+sql);
								}
							}
							continue;
							
						}
						//
					}
	
					String sTableName = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_TABLENAME);
					logger.log(Level.FINEST,"[init] "+sKey+" - tablename:"+sTableName+" ... ");
					
					String sSQL = null;
					
					if(sTableName==null)
					{
						sSQL = mapCrudConfig.get(JsonCrudConfig._PROP_KEY_SQL);
						logger.log(Level.FINEST,"[init] "+sKey+" - sql:"+sSQL+" ... ");
					}
					
					if(sSQL==null && sTableName!=null && sTableName.trim().length()>0)
					{
						sSQL = "SELECT * FROM "+sTableName+" WHERE 1=2";
					}

					if(sSQL!=null)
					{
						
						Map<String, DBColMeta> mapCols = getTableMetaDataBySQL(sKey, sSQL);
						if(mapCols!=null)
						{	
							mapTableCols.put(sKey, mapCols);
							
							logger.log(Level.FINEST, sKey+"."+sTableName+" : "+mapCols.size()+" cols meta loaded.");
							
							String sExludeNonMappedFields 		= mapCrudConfig.get(JsonCrudConfig._PROP_KEY_EXCLUDE_NON_MAPPED_FIELDS);
							boolean isExcludeNonMappedFields 	= "true".equalsIgnoreCase(sExludeNonMappedFields);
							
							for(String sColName : mapCols.keySet())
							{
								//No DB Mapping configured
								if(mapCrudCol2Json.get(sColName)==null && !isExcludeNonMappedFields)
								{
									mapCrudJson2Col.put(sColName, sColName);
									mapCrudCol2Json.put(sColName, sColName);
								}
							}
						}
					}
					
					mapJson2ColName.put(sKey, mapCrudJson2Col);
					mapColName2Json.put(sKey, mapCrudCol2Json);
					mapJson2Sql.put(sKey, mapCrudJsonSelectSql);
					//
				}
			}
		}
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
			Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(aCrudKey);
			String sCrudTableName = mapCrudConfig.get(jsoncrudConfig._PROP_KEY_TABLENAME);
			
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
									sbErrInfo.append(" - '").append(col.getCollabel()).append("' cannot be empty. ").append(col);
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
									sbErrInfo.append(" - '").append(col.getCollabel()).append("' invalid type,");
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
										sbErrInfo.append(" - '").append(col.getCollabel()).append("' exceed allowed size,");
										sbErrInfo.append(" expect:").append(col.getColsize());
										sbErrInfo.append(" actual:").append(sVal.length()).append(". ").append(col);
									}
									listErr.add(sbErrInfo.toString());
								}
							}
							///// Check if Data is autoincremental //////
							if(col.getColautoincrement())
							{
								//Only check validate system if it's same table operation and same col name
								if(col.getTablename().equalsIgnoreCase(sCrudTableName) && col.getCollabel().equals(col.getColname()))
								{
									// 
									sbErrInfo.setLength(0);
									sbErrInfo.append(JsonCrudConfig.ERRCODE_SYSTEM_FIELD);								
									if(isDebugMode)
									{
										sbErrInfo.append(" - '").append(col.getCollabel()).append("' not allowed (auto increment field). ").append(col);
									}
									listErr.add(sbErrInfo.toString());
								}
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
	
	
	public JSONObject castJson2DBVal(String aCrudKey, JSONObject aDataObj) throws JsonCrudException
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
	
	public Object castJson2DBVal(String aCrudKey, final String aOrigJsonName, Object aVal) throws JsonCrudException
	{
		if(aVal == null)
			return JSONObject.NULL;
		
		if(!(aVal instanceof String))
			return aVal;
		
		//multi-values
		if(aOrigJsonName.toLowerCase().indexOf(".in")>-1)
			return aVal;
		
		//only cast string value
		String sJsonName = removeJsonNameFilters(aOrigJsonName);
		
		Object oVal = aVal;
		DBColMeta col = getDBColMetaByJsonName(aCrudKey, sJsonName);
		if(col!=null)
		{
			boolean isFormatOk = false;
			String sErrSubject 	= null;
			String sErrReason 	= null;
			
			String sVal = String.valueOf(aVal);
			
			if(col.getColnullable() && JSONFILTER_VALUE_NULL.equalsIgnoreCase(sVal))
			{
				return JSONObject.NULL;
			}
			else if(col.isNumeric())
			{
				try {
					if(sVal.indexOf('.')>-1)
						oVal = Double.parseDouble(sVal);
					else
						oVal = Long.parseLong(sVal);
					
					isFormatOk = true;
				}
				catch(NumberFormatException numEx)
				{
					sErrReason 	= "Expecting numeric value !";
					sErrSubject = sJsonName+":"+sVal;
					logger.log(Level.FINEST, numEx.getMessage(), numEx);
				}
			}
			else if(col.isBoolean() || col.isBit())
			{
				if(sVal.length()==1)
				{
					isFormatOk = ("1".equals(sVal) || "0".equals(sVal));
				}
				else
				{
					isFormatOk = ("true".equalsIgnoreCase(sVal) || "false".equalsIgnoreCase(sVal));
				}
				
				if(isFormatOk)
				{
					oVal = Boolean.parseBoolean(sVal);
				}
				else
				{
					sErrReason 	= "Expecting boolean value !";
					sErrSubject = sJsonName+":"+sVal;
				}
			}
			
			//////
			if(!isFormatOk)
			{
				if(col.isString())
				{
					oVal = sVal;
				}
				else if(sErrReason!=null)
				{
					if("NaN".equalsIgnoreCase(sVal))
					{
						return JSONObject.NULL;
					}
					
					JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_INVALID_TYPE, sErrReason);
					if(sErrSubject!=null)
						e.setErrorSubject(sErrSubject);
					throw e;
				}
			}
		}
		return oVal;
	}
	
	public DBColMeta getDBColMetaByColName(String aCrudKey, String aColName)
	{
		Map<String,DBColMeta> cols = mapTableCols.get(aCrudKey);
		for(DBColMeta col : cols.values())
		{
			if(col.getCollabel().equalsIgnoreCase(aColName))
			{
				return col;
			}
		}
		return null;
	}
	
	public DBColMeta getDBColMetaByJsonName(String aCrudKey, String aJsonName)
	{
		aJsonName = removeJsonNameFilters(aJsonName);
		//
		Map<String,DBColMeta> cols = mapTableCols.get(aCrudKey);
		if(cols!=null)
		{
			Map<String,String> mapCol2Json = mapColName2Json.get(aCrudKey);
			if(mapCol2Json!=null && mapCol2Json.size()>0)
			for(DBColMeta col : cols.values())
			{
				String sColJsonName = mapCol2Json.get(col.getCollabel());
				if(sColJsonName!=null && sColJsonName.equalsIgnoreCase(aJsonName))
				{
					return col;
				}
			}
		}
		return null;
	}
	
	private Map<String, DBColMeta> getTableMetaDataBySQL(String aCrudKey, String aSQL) throws JsonCrudException
	{
		if(aSQL==null || aSQL.length()==0)
			return null;
		
		Map<String, DBColMeta> mapDBColJson = new HashMap<String, DBColMeta>();
		String sSQL = aSQL.replaceAll(" [Ww][Hh][Ee][Rr][Ee] ", " WHERE 1=2 AND ");

		if(sSQL.indexOf(" WHERE ")==-1)
		{
			// ORDER BY ?
			sSQL = sSQL.replaceAll(" [Oo][Rr][Dd][Ee][Rr] [Bb][Yy] ", " WHERE 1=2 ORDER BY ");
		}
		
		if(sSQL.indexOf(" WHERE ")==-1)
		{
			// GROUP BY ?
			sSQL = sSQL.replaceAll(" [Gg][Rr][Oo][Uu][Pp] [Bb][Yy] ", " WHERE 1=2 GROUP BY ");
		}
		
		Map<String, String> mapConfig = jsoncrudConfig.getConfig(aCrudKey);
		String sJdbcKey = mapConfig.get("dbconfig");
		
		JdbcDBMgr jdbcMgr = mapDBMgr.get(sJdbcKey);
		
		Connection conn = null;
		PreparedStatement stmt	= null;
		ResultSet rs = null;
		
		try{
			conn = jdbcMgr.getConnection();
			stmt = conn.prepareStatement(sSQL);
			conn.setAutoCommit(false);
			stmt.setFetchSize(1);
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
				coljson.setCollabel(meta.getColumnLabel(idx));
				coljson.setColname(meta.getColumnName(idx));
				coljson.setColclassname(meta.getColumnClassName(idx));
				coljson.setColtypename(meta.getColumnTypeName(idx));
				coljson.setColtype(String.valueOf(meta.getColumnType(idx)));
				coljson.setColsize(meta.getColumnDisplaySize(idx));
				coljson.setColnullable(ResultSetMetaData.columnNullable == meta.isNullable(idx));
				coljson.setColautoincrement(meta.isAutoIncrement(idx));
				//
				String sJsonName = mapColJsonName.get(coljson.getCollabel());
				if(sJsonName!=null)
				{
					coljson.setJsonname(sJsonName);
				}
				//
				mapDBColJson.put(coljson.getCollabel(), coljson);
			}
		}
		catch(SQLException ex)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, ex);
		}
		finally
		{
			if(conn!=null)
			{
				try {
					conn.setAutoCommit(true);
				} catch (SQLException e) {
					//keep quiet
				}
			}
			jdbcMgr.closeQuietly(conn, stmt, rs);
		}

		if(mapDBColJson.size()==0)
		{
			return null;
		}
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
	
	public void setDebugMode(String aCrudConfigKey, boolean isDebugMode)
	{
		Map<String, String> mapCrudConfig = jsoncrudConfig.getConfig(aCrudConfigKey);
		if(mapCrudConfig!=null)
		{
			if(isDebugMode)
			{
				mapCrudConfig.put(aCrudConfigKey+"."+JsonCrudConfig._PROP_KEY_DEBUG, "true");
			}
			else
			{
				mapCrudConfig.put(aCrudConfigKey+"."+JsonCrudConfig._PROP_KEY_DEBUG, "false");
			}
		}
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
				} catch (SQLException sqlEx) {
					JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
					e.setErrorDebugInfo("sql:"+sbObjSQL2.toString()+", params:"+listParamsToString(obj2));
					throw e;
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
				JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_SQLEXCEPTION, sqlEx);
				e.setErrorDebugInfo("sql:"+sbObjSQL2.toString()+", params:"+listParamsToString(aListParams));
				throw e;
			}
		}
		return lAffectedRow;
	}
	
	
	private List<Object[]> getInsertSubQueryParams(Map<String, String> aCrudCfgMap, JSONObject aJsonParentData, String aJsonName) throws JsonCrudException
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
		{
			JsonCrudException e = new JsonCrudException(JsonCrudConfig.ERRCODE_JSONCRUDCFG, "No object mapping found !");
			e.setErrorSubject(aJsonName);
			throw e;
		}
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
	
	private String getFilterOperator(String aJsonName)
	{
		String sOperator = null;
		int iPos = aJsonName.lastIndexOf(".");
		if(iPos >-1)
		{
			sOperator = aJsonName.substring(iPos, aJsonName.length());
			if(!listFilterOperator.contains(sOperator))
			{
				sOperator = null;
			}
		}
		return sOperator;
	}
	
	private String removeJsonNameFilters(final String aJsonName)
	{
		String sJsonName = aJsonName;
		
		String sOperator = getFilterOperator(sJsonName);
			
		if(sOperator!=null)
		{
			sJsonName =  aJsonName.substring(0, aJsonName.length()-sOperator.length());
			if(sOperator!=null)
				sJsonName = removeJsonNameFilters(sJsonName);
		}
		return sJsonName;
	}
	
	public JSONObject convertCol2Json(String aCrudKey, JSONObject aJSONObject, boolean excludeIfNoMapped)
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
			{
				if(excludeIfNoMapped)
					continue;
				sMappedKey = sColName;
			}
			
			Object obj = aJSONObject.get(sColName);
			
			if(obj instanceof JSONObject)
			{
				obj = convertCol2Json(aCrudKey, (JSONObject) obj, excludeIfNoMapped);
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
						obj2 = convertCol2Json(aCrudKey, (JSONObject) obj2, excludeIfNoMapped);
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
	
	private void updLastUpdatedTimestamp(String aCrudKey, long aUpdatedTime)
	{
		Long lLastUpdated = mapLastUpdates.get(aCrudKey);
		if(lLastUpdated==null || aUpdatedTime>lLastUpdated.longValue())
		{
			mapLastUpdates.put(aCrudKey, aUpdatedTime);
		}
	}
	
	public long getLastUpdatedTimestamp(String aCrudKey)
	{
		return mapLastUpdates.get(aCrudKey);
	}
	
	//////////////////////////////////////////////////////////////
	/**
	public static void main(String args[]) throws JsonCrudException
	{
		Map<String, String> mapTests = new LinkedHashMap<String,String>();
		
		mapTests.put("j=0", "j");
		mapTests.put("json1=0", "json1");
		mapTests.put("json1.2=0", "json1.2");
		mapTests.put("json.name1=0", "json.name1");
		mapTests.put("json.name.1.2=0", "json.name.1.2");
		mapTests.put("json.name1.{2}.3=0", "json.name.{2}.3");

		
		//Test Regex
		Pattern pattJsonName = Pattern.compile("("+_JSON_ATTRNAME+")?\\=");
		
		
		int i=1;
		for(String sTest : mapTests.keySet())
		{
			String sExpectedText = mapTests.get(sTest);
			
			System.out.println((i++)+". '"+sTest+"' ... ");
			Matcher m = pattJsonName.matcher(sTest);
			boolean isMatched = m.find();
			
			if(isMatched)
			{
				//System.out.println("    - groundCount = "+m.groupCount());
				System.out.print("    -> ('"+m.group(1)+"' == '"+sExpectedText+"') = ");
				
				isMatched = sExpectedText.equals(m.group(1));
				
			}
			
			if(isMatched)
				System.out.println(PRN_GREEN+"true"+PRN_RESET);
			else
				System.out.println(PRN_RED+"false"+PRN_RESET);
		}
	}
		**/

}