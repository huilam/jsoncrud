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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hl.common.PropUtil;


public class JsonCrudConfig {
	
	public static String _PROP_FILENAME 		= "jsoncrud.properties";
	//
	public static String _PROP_KEY_CRUD 		= "crud";
	
	public final static String _PAGINATION_CONFIGKEY = "list.pagination";
	public static String _LIST_META 		= "meta";
	public static String _LIST_RESULT 		= "result";
	public static String _LIST_TOTAL 		= "total";
	public static String _LIST_FETCHSIZE 	= "fetchsize";
	public static String _LIST_START 		= "start";
	public static String _LIST_SORTING 		= "sorting";
	
	public final static String _DB_VALIDATION_ERRCODE_CONFIGKEY = "dbschema.validation_errcode";
	public static String ERRCODE_NOT_NULLABLE 		= "not_nullable";
	public static String ERRCODE_EXCEED_SIZE 		= "exceed_size";
	public static String ERRCODE_INVALID_TYPE		= "invalid_type";
	public static String ERRCODE_SYSTEM_FIELD		= "system_field";
	
	public final static String _JSONCRUD_FRAMEWORK_ERRCODE_CONFIGKEY = "jsoncrud.framework_errcode";
	public static String ERRCODE_PLUGINEXCEPTION	= "plugin_exception";
	public static String ERRCODE_DBCONNEXCEPTION		= "dbconn_exception";
	public static String ERRCODE_SQLEXCEPTION		= "sql_exception";
	
	public static String ERRCODE_INVALIDFORMAT		= "invalid_format_exception";
	
	public static String ERRCODE_JSONCRUDCFG		= "invalid_jsoncrudcfg";
	
	public static String ERRCODE_INVALID_FILTERS	= "invalid_filters";
	public static String ERRCODE_INVALID_RETURNS	= "invalid_returns";
	public static String ERRCODE_INVALID_SORTING	= "invalid_sorting";
	public static String ERRCODE_INVALID_PAGINATION	= "invalid_pagination";
	
	//
	public static char[] SQLLIKE_ESCAPE_CHARS		= new char[] {'`','|','#'};
	//
	public static String _PROP_KEY_JDBC_CLASSNAME 	= "classname";
	public static String _PROP_KEY_JDBC_UID 		= "uid";
	public static String _PROP_KEY_JDBC_PWD 		= "pwd";
	public static String _PROP_KEY_JDBC_URL 		= "url";
	public static String _PROP_KEY_JDBC_CONNPOOL	= "connpool";
	
	public static String _PROP_KEY_JDBC_FETCHSIZE				= "fetchsize";
	public static String _PROP_KEY_JDBC_MAXCONNS				= "maxconn";
	public static String _PROP_KEY_JDBC_CONN_WAIT_INTERVAL_MS 	= "conn.wait-interval.ms";
	public static String _PROP_KEY_JDBC_CONN_TIMEOUT_MS			= "conn.timeout.ms";
	
	//
	public static String _PROP_KEY_JDBC			= "jdbc";
	public static String _PROP_KEY_DBCONFIG		= "dbconfig";
	public static String _PROP_KEY_TABLENAME 	= "tablename";
	public static String _PROP_KEY_JSON 		= "jsonattr";
	public static String _PROP_KEY_COLNAME 		= "colname";
	public static String _PROP_KEY_SQL			= "sql";
	//public static String _PROP_KEY_FORMAT		= "format";
	public static String _PROP_KEY_DEBUG 		= "debug";
	public static String _PROP_KEY_RETRIEVEONLY = "retrieve.only";
	
	public static String _PROP_KEY_EXCLUDE_NON_MAPPED_FIELDS	= "exclude.non-mapped-fields";
	public static String _PROP_KEY_EXCEPTION_ON_UNKNOWN_ATTR	= "exception.on-unknown-jsonattr";
	public static String _PROP_KEY_CHILD_MAPPING	= "mapping";
	public static String _PROP_KEY_CHILD_INSERTSQL	= "insert.sql";
	//
	public static Pattern patJsonDaoKey 		= null;
	
	private Map<String, Map<String, String>> mapJsonCrudConfig = null;
	//

	public JsonCrudConfig(String aPropFileName) throws IOException
	{
		init(aPropFileName);
	}
	
	public JsonCrudConfig(Properties aProperties) throws IOException
	{
		init(aProperties);
	}
	
	public void init(String aPropFilename) throws IOException
	{		
		Properties props = null;
		if(aPropFilename!=null && aPropFilename.trim().length()>0)
		{
			props = PropUtil.loadProperties(aPropFilename);
		}
		
		/////////
		if(props==null || props.size()==0)
		{
			props = PropUtil.loadProperties(_PROP_FILENAME);
		}
		
		init(props);
	}
	
	public void init(Properties aProperties) throws IOException
	{		
		mapJsonCrudConfig = new HashMap<String,Map<String, String>>();
		patJsonDaoKey = Pattern.compile("(.+?\\..+?)\\.");
		
		Properties prop = new Properties();
		if(aProperties!=null)
		{
			for(Object oKey : aProperties.keySet())
			{
				String sKey = oKey.toString();
				String sVal = aProperties.getProperty(sKey);
				prop.setProperty(sKey, sVal.trim());
			}
		}
		loadProp(prop);
	}
	
	public boolean isValidCrudSortingDirection(String aSortOperator)
	{
		if(aSortOperator==null)
			return false;
		return "asc".equalsIgnoreCase(aSortOperator) || "desc".equalsIgnoreCase(aSortOperator);
	}
	
	
	public void loadProp(Properties aProp) throws IOException
	{
		Iterator iter = aProp.keySet().iterator();
		while(iter.hasNext())
		{
			String sOrgkey = (String) iter.next();
			
			Matcher m = patJsonDaoKey.matcher(sOrgkey);
			if(m.find())
			{
				String sPrefixKey = m.group(1);
				Map<String, String> mapConfig = mapJsonCrudConfig.get(sPrefixKey);
				if(mapConfig==null)
				{
					mapConfig = new HashMap<String, String>();
				}
				String key = sOrgkey.substring(sPrefixKey.length()+1);
				mapConfig.put(key, aProp.getProperty(sOrgkey));
				//
				mapJsonCrudConfig.put(sPrefixKey, mapConfig);
			}
		}
	}
	
	public Map<String, String> getConfig(String sConfigKey)
	{
		Map<String, String> map = new HashMap<String, String>();
		if(mapJsonCrudConfig!=null && mapJsonCrudConfig.get(sConfigKey)!=null)
		{
			map.putAll(mapJsonCrudConfig.get(sConfigKey));
		}
		return Collections.unmodifiableMap(map);
	}
	
	public Map<String, String> getAllConfig()
	{
		Map<String, String> mapAlls = new HashMap<String, String>();
		
		if(mapJsonCrudConfig!=null)
		{
			for(String sKey : mapJsonCrudConfig.keySet())
			{
				Map<String, String> map2 = mapJsonCrudConfig.get(sKey);
				for(String sKey2 : map2.keySet())
				{
					mapAlls.put(sKey+"."+sKey2, map2.get(sKey2));
				}
			}
		}
		
		return Collections.unmodifiableMap(mapAlls);
	}
	
	public String[] getConfigCrudKeys()
	{
		Set<String> setKeys = mapJsonCrudConfig.keySet();
		return setKeys.toArray(new String[setKeys.size()]);
	}
	
	
}