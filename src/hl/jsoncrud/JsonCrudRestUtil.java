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

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonCrudRestUtil {
	
	private static CRUDMgr crudmgr 				= new CRUDMgr();
	
	public static JSONObject create(String aCrudKey, JSONObject aJSONObject) throws Throwable
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
	
	public static JSONArray create(String aCrudKey, JSONArray aJsonInputArray) throws Throwable
	{
		JSONArray jsonOutputArray = new JSONArray();
		
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		boolean debug = crudmgr.isDebugMode(sConfigKey);
		
		if(aJsonInputArray==null || aJsonInputArray.length()==0)
		{
			return jsonOutputArray;
		}
			
		for(Object obj : aJsonInputArray)
		{
			JSONObject jsonInput = (JSONObject) obj;
			
    		JSONObject jsonOutput = crudmgr.create(sConfigKey, jsonInput);
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
			int iStartFrom, int iFetchSize) throws Exception
	{
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		
		JSONObject jsonOutput = crudmgr.retrieve(
				sConfigKey, aSQL, aObjParams, iStartFrom, iFetchSize);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		try {
			jsonOutput.get(crudmgr._LIST_RESULT);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(crudmgr._LIST_RESULT, new JSONArray());
		}
		
		try {
			jsonOutput.get(crudmgr._LIST_META);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(crudmgr._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
	
	public static JSONObject retrieveList(
			String aCrudKey, JSONObject aJsonWhere,
			int iStartFrom, int iFetchSize, List<String> listOrderBy, boolean isOrderDesc) throws Exception
	{

		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		boolean debug = crudmgr.isDebugMode(sConfigKey);

		JSONObject jsonOutput = crudmgr.retrieve(
				sConfigKey, aJsonWhere, iStartFrom, iFetchSize, 
				listOrderBy.toArray(new String[listOrderBy.size()]), isOrderDesc);
		
		if(jsonOutput==null)
		{
			jsonOutput = new JSONObject();
		}
		
		try {
			jsonOutput.get(crudmgr._LIST_RESULT);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(crudmgr._LIST_RESULT, new JSONArray());
		}
		
		try {
			jsonOutput.get(crudmgr._LIST_META);
		}
		catch(JSONException ex)
		{
			jsonOutput.put(crudmgr._LIST_META, new JSONObject());
		}

		return jsonOutput;
	}
    
	public static JSONObject retrieve(String aCrudKey, JSONObject aJsonWhere) throws Exception
	{
    	
		String sConfigKey = JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		boolean debug = crudmgr.isDebugMode(sConfigKey);
   		
		return crudmgr.retrieveFirst(sConfigKey, aJsonWhere);
	}

	public static JSONArray update(String aCrudKey,	JSONObject aJsonData, JSONObject aJsonWhere) throws Exception
	{
   		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		boolean debug 		= crudmgr.isDebugMode(sConfigKey);
		
		return  crudmgr.update(sConfigKey, aJsonData, aJsonWhere);
	}
    
	public static JSONArray delete(String aCrudKey, JSONObject aJsonWhere) throws Exception
	{
		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		boolean debug 		= crudmgr.isDebugMode(sConfigKey);
		
		return crudmgr.delete(sConfigKey, aJsonWhere);
	}
	
	public static boolean isDebugEnabled(String aCrudKey)
	{
		String sConfigKey 	= JsonCrudConfig._PROP_KEY_CRUD+"."+aCrudKey;
		return crudmgr.isDebugMode(sConfigKey);
	}

	public static JSONObject getCrudConfigJson(String aPrefix)
    {
   		JSONObject jsonCfg = new JSONObject();
   	    Map<String,String> map = crudmgr.getAllConfig();
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
		return crudmgr.getVersionInfo();
	}
	
	public static CRUDMgr getCRUDMgr()
	{
		return crudmgr;
	}
    
}