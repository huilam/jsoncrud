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
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonCrudRestUtil {
	
	private static CRUDMgr _crudmgr 	= null;
	
	public static JSONObject create(String aCrudKey, JSONObject aJSONObject) throws Exception
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
	
	public static JSONArray create(String aCrudKey, JSONArray aJsonInputArray) throws Exception
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
			long iStartFrom, long iFetchSize) throws Exception
	{
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		
		JSONObject jsonOutput = getCRUDMgr().retrieve(
				sConfigKey, aSQL, aObjParams, iStartFrom, iFetchSize);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		try {
			jsonOutput.get(getCRUDMgr()._LIST_RESULT);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(getCRUDMgr()._LIST_RESULT, new JSONArray());
		}
		
		try {
			jsonOutput.get(getCRUDMgr()._LIST_META);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(getCRUDMgr()._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			long iStartFrom, long iFetchSize) throws Exception
	{
		return retrieveList(aCrudKey, aJsonWhere, iStartFrom, iFetchSize, null, false);
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			long iStartFrom, long iFetchSize, List<String> listOrderBy, boolean isOrderDesc) throws Exception
	{
		CRUDMgr crudMgr = getCRUDMgr();
		
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug = crudMgr.isDebugMode(sConfigKey);
		
		if(listOrderBy==null)
			listOrderBy = new ArrayList<String>();

		JSONObject jsonOutput = crudMgr.retrieve(
				sConfigKey, aJsonWhere, iStartFrom, iFetchSize, 
				listOrderBy.toArray(new String[listOrderBy.size()]), isOrderDesc);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		if(!jsonOutput.has(crudMgr._LIST_RESULT))
		{
			jsonOutput.put(crudMgr._LIST_RESULT, new JSONArray());
		}
		
		if(!jsonOutput.has(crudMgr._LIST_META))
		{
			jsonOutput.put(crudMgr._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
	
	public static JSONObject getListMeta(JSONObject aJsonObject)
	{
		CRUDMgr crudMgr = getCRUDMgr();
		
		if(aJsonObject.has(crudMgr._LIST_META))
		{
			return aJsonObject.getJSONObject(crudMgr._LIST_META);
		}
		else
		{
			return null;
		}
	}
	
	public static JSONArray getListResult(JSONObject aJsonObject)
	{
		CRUDMgr crudMgr = getCRUDMgr();
		
		if(aJsonObject.has(crudMgr._LIST_RESULT))
		{
			return aJsonObject.getJSONArray(crudMgr._LIST_RESULT);
		}
		else
		{
			return null;
		}
	}
    
	public static JSONObject retrieveFirst(String aCrudKey, JSONObject aJsonWhere) throws Exception
	{
    	
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug = getCRUDMgr().isDebugMode(sConfigKey);
   		
		return getCRUDMgr().retrieveFirst(sConfigKey, aJsonWhere);
	}

	public static JSONArray update(String aCrudKey,	JSONObject aJsonData, JSONObject aJsonWhere) throws Exception
	{
   		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		//boolean debug 		= getCRUDMgr().isDebugMode(sConfigKey);
		
		return  getCRUDMgr().update(sConfigKey, aJsonData, aJsonWhere);
	}
    
	public static JSONArray delete(String aCrudKey, JSONObject aJsonWhere) throws Exception
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
	
	public static String getJsonCrudVersion()
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
		if(_crudmgr==null)
		{
			_crudmgr = new CRUDMgr();
		}
		return _crudmgr;
	}
    
}