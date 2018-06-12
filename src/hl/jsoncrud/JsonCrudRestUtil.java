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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonCrudRestUtil {
	
	private static Map<String, CRUDMgr> mapCrudMgrs = new HashMap<String, CRUDMgr>();
	private static Logger logger = Logger.getLogger(JsonCrudRestUtil.class.getName());
	
	
	public static JSONObject create(String aCrudKey, String aJsonContent) throws JsonCrudException
	{
		JSONObject jsonCreated = null;
		
		if(aJsonContent!=null && aJsonContent.length()>0)
		{
			aJsonContent = aJsonContent.trim();
			try {
				if(aJsonContent.startsWith("{") && aJsonContent.endsWith("}"))
				{
					jsonCreated = create(aCrudKey, new JSONObject(aJsonContent));
				}
				else if(aJsonContent.startsWith("[") && aJsonContent.endsWith("]"))
				{
					JSONArray jsonOutputArray = create(aCrudKey, new JSONArray(aJsonContent));
			    	if(jsonOutputArray.length()>0)
			    	{
			    		jsonCreated = jsonOutputArray.getJSONObject(0);
			    	}
				}
				else
				{
					throw new JSONException("Invalid content : "+aJsonContent);
				}
			}catch(JSONException ex)
			{
				throw new JsonCrudException(JsonCrudConfig.ERRCODE_INVALIDFORMAT,
						"Invalid content : "+aJsonContent, ex);
			}
		}
		return jsonCreated;
	}
	
	
	public static JSONObject create(String aCrudKey, JSONObject aJSONObject) throws JsonCrudException
	{
    	if(aJSONObject==null)
    		return aJSONObject;
    	
    	JSONArray jsonInputArray = new JSONArray();
    	jsonInputArray.put(aJSONObject);
    	
    	JSONArray jsonOutputArray = create(aCrudKey, jsonInputArray);
    	if(jsonOutputArray.length()>0)
    		return (JSONObject) jsonOutputArray.get(0);
    	else 
    		return null;
	}
	
	public static JSONArray create(String aCrudKey, JSONArray aJsonInputArray) throws JsonCrudException
	{
		JSONArray jsonOutputArray = new JSONArray();
		
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug = getCRUDMgr().isDebugMode(sConfigKey);
		
		if(aJsonInputArray==null || aJsonInputArray.length()==0)
		{
			return jsonOutputArray;
		}
			
		for(Object obj : aJsonInputArray)
		{
			JSONObject jsonInput = (JSONObject) obj;
			
    		JSONObject jsonOutput = getCRUDMgr().create(sConfigKey, jsonInput);
    		if(jsonOutput==null)
    		{
    			jsonOutput = new JSONObject();
    		}
    		jsonOutputArray.put(jsonOutput);
		}
		
		return jsonOutputArray;
	}
        
	public static JSONObject retrieveList(
			String aCrudKey, String aSQL, Object[] aObjParams,
			long iStartFrom, long iFetchSize) throws JsonCrudException
	{
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		
		JSONObject jsonOutput = getCRUDMgr().retrieveBySQL(
				sConfigKey, aSQL, aObjParams, iStartFrom, iFetchSize);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		try {
			jsonOutput.get(JsonCrudConfig._LIST_RESULT);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(JsonCrudConfig._LIST_RESULT, new JSONArray());
		}
		
		try {
			jsonOutput.get(JsonCrudConfig._LIST_META);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(JsonCrudConfig._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			long iStartFrom, long iFetchSize) throws JsonCrudException
	{
		return retrieveList(aCrudKey, aJsonWhere, iStartFrom, iFetchSize, null, null);
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			long iStartFrom, long iFetchSize,
			List<String> listSorting,
			List<String> listReturns) throws JsonCrudException
	{
		return retrieveList(aCrudKey, aJsonWhere, iStartFrom, iFetchSize, listSorting, listReturns, false);
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			long iStartFrom, long iFetchSize, 
			List<String> listSorting,
			List<String> listReturns,
			boolean isReturnsExclude) throws JsonCrudException
	{
		CRUDMgr crudMgr = getCRUDMgr();
		
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug = crudMgr.isDebugMode(sConfigKey);
		
		if(listSorting==null)
			listSorting = new ArrayList<String>();
		
		if(listReturns==null)
			listReturns = new ArrayList<String>();
		
		JSONObject jsonOutput = crudMgr.retrieve(
				sConfigKey, aJsonWhere, iStartFrom, iFetchSize, 
				listSorting.toArray(new String[listSorting.size()]),
				listReturns.toArray(new String[listReturns.size()]),
				isReturnsExclude);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		if(!jsonOutput.has(JsonCrudConfig._LIST_RESULT))
		{
			jsonOutput.put(JsonCrudConfig._LIST_RESULT, new JSONArray());
		}
		
		if(!jsonOutput.has(JsonCrudConfig._LIST_META))
		{
			jsonOutput.put(JsonCrudConfig._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
	
	public static JSONObject getListMeta(JSONObject aJsonObject)
	{
		if(aJsonObject.has(JsonCrudConfig._LIST_META))
		{
			return aJsonObject.getJSONObject(JsonCrudConfig._LIST_META);
		}
		else
		{
			return null;
		}
	}
	
	public static JSONArray getListResult(JSONObject aJsonObject)
	{
		if(aJsonObject.has(JsonCrudConfig._LIST_RESULT))
		{
			return aJsonObject.getJSONArray(JsonCrudConfig._LIST_RESULT);
		}
		else
		{
			return null;
		}
	}
    
	public static JSONObject retrieveFirst(String aCrudKey, JSONObject aJsonWhere) throws JsonCrudException
	{
    	
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug = getCRUDMgr().isDebugMode(sConfigKey);
   		
		return getCRUDMgr().retrieveFirst(sConfigKey, aJsonWhere);
	}

	public static JSONArray update(String aCrudKey,	JSONObject aJsonData, JSONObject aJsonWhere) throws JsonCrudException
	{
   		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug 		= getCRUDMgr().isDebugMode(sConfigKey);
		
		return  getCRUDMgr().update(sConfigKey, aJsonData, aJsonWhere);
	}
    
	public static JSONArray delete(String aCrudKey, JSONObject aJsonWhere) throws JsonCrudException
	{
		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug 		= getCRUDMgr().isDebugMode(sConfigKey);
		
		return getCRUDMgr().delete(sConfigKey, aJsonWhere);
	}
	
	public static boolean isDebugEnabled(String aCrudKey)
	{
		String sConfigKey = null;
		
		if(!aCrudKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD+"."))
		{
			sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		}
		else
		{
			sConfigKey = aCrudKey;
		}
		
		return getCRUDMgr().isDebugMode(sConfigKey);
	}

	public static JSONObject getCrudConfigJson(String aPrefix)
    {
   		JSONObject jsonCfg = new JSONObject();
   	    Map<String,String> map = getCRUDMgr().getAllConfig();
   	    if(map!=null)
   	    {
   	    	for(String sKey : map.keySet())
   	    	{
   	    		if(aPrefix!=null && aPrefix.trim().length()>0)
   	    		{
   	    			if(!sKey.startsWith(aPrefix))
   	    				continue;
   	    		}
   	    		jsonCfg.put(sKey, map.get(sKey));
   	    	}
   	    }
   	    return jsonCfg;
    }
	
	public static JSONObject getJsonCrudVersion()
	{
		return getCRUDMgr().getVersionInfo();
	}
	
	public static boolean isJsonArray(String aJsonString)
	{
		if(aJsonString==null)
			return false;
		
		aJsonString = aJsonString.trim();
		return (aJsonString.startsWith("[") && aJsonString.startsWith("]"));
	}
	
	public static boolean isJsonObject(String aJsonString)
	{
		if(aJsonString==null)
			return false;
		
		aJsonString = aJsonString.trim();
		return (aJsonString.startsWith("{") && aJsonString.startsWith("}"));
	}
	
	public static CRUDMgr getCRUDMgr()
	{
		return getCRUDMgr(null);
	}
	
	public static CRUDMgr registerCRUDMgr(String aNameSpace, Properties aProperties) throws JsonCrudException
	{
		if(aNameSpace==null || aNameSpace.trim().length()==0)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_PLUGINEXCEPTION, 
					"Namespace for CRUDMgr cannot be null ! - "+aNameSpace);
		}
		
		CRUDMgr crudmgr = new CRUDMgr(aProperties);
		return mapCrudMgrs.put(aNameSpace, crudmgr);
	}
		
	public static CRUDMgr unregisterCRUDMgr(String aNameSpace) throws JsonCrudException
	{
		if(aNameSpace==null || aNameSpace.trim().length()==0)
		{
			throw new JsonCrudException(JsonCrudConfig.ERRCODE_PLUGINEXCEPTION, 
					"Namespace for CRUDMgr cannot be null ! - "+aNameSpace);
		}
		
		return mapCrudMgrs.remove(aNameSpace);
	}
	
	public static CRUDMgr getCRUDMgr(String aNameSpace)
	{
		CRUDMgr crudmgr = null;
		
		if(aNameSpace==null || aNameSpace.trim().length()==0)
		{
			aNameSpace = "CORE";
		}
		
		synchronized(mapCrudMgrs)
		{						
			crudmgr = mapCrudMgrs.get(aNameSpace);
			
			if(crudmgr==null)
			{
				crudmgr = new CRUDMgr((String)null);
				
				if(crudmgr!=null)
				{
					mapCrudMgrs.put(aNameSpace, crudmgr);
				}
			}
			
		}
		return crudmgr;
	}
    
	
	public static double getCrudConfigNumbericVal(String aCrudKey, String aConfigKey, double aDefaultVal) throws JsonCrudException
	{
		if(!aCrudKey.startsWith(JsonCrudConfig._PROP_KEY_CRUD+"."))
		{
			aCrudKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		}
		
		Map<String, String> mapConfig = getCRUDMgr(null).getCrudConfigs(aCrudKey);
		if(mapConfig!=null)
		{
			String sVal = mapConfig.get(aConfigKey);
			if(sVal==null)
			{
				//logger.log(Level.WARNING,"No configuration value:"+sVal+" for crudkey:"+aCrudKey+", key:"+aConfigKey);
				return aDefaultVal;
			}
			
			try {
				return Double.parseDouble(sVal);
			}catch(NumberFormatException ex)
			{
				logger.log(Level.WARNING,"Invalid configuration value:"+sVal+" for crudkey:"+aCrudKey+", key:"+aConfigKey);
			}
		}		
		return aDefaultVal;
	}
}