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

import java.util.Map;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;

import hl.jsoncrud.CRUDMgr;

public class CRUDMgrTest {
	
	private void test1_CRUD(CRUDMgr m) throws JsonCrudException
	{
		JSONObject jsonData 	= null;
		JSONObject jsonWhere 	= null;
		JSONObject jsonResult 	= null;
		JSONArray jsonArrResult = null;
		
		System.out.println();
		System.out.println("1. CRUD");
		jsonData = new JSONObject();
		jsonData.put("appNamespace", "jsoncrud-framework");
		jsonData.put("moduleCode", "unit-test");
		jsonData.put("enabled", false);
		jsonResult = m.create("crud.jsoncrud_cfg", jsonData);
		//
		long id = jsonResult.getLong("cfgId");
		jsonData = new JSONObject();
		jsonData.put("cfgId", id);
		jsonData.put("key", "testkey01_");
		jsonData.put("value", "testvalue001_");
		jsonData.put("enabled", true);
		jsonData.put("displaySeq", 1);
		m.create("crud.jsoncrud_cfg_values", jsonData);
		//
		jsonData.put("key", "testkey02_");
		jsonData.put("value", "testvalue|%002_");
		jsonData.put("displaySeq", 200);
		m.create("crud.jsoncrud_cfg_values", jsonData);
		//
		jsonData.put("key", "testkey03_");
		jsonData.put("value", "testvalue003");
		jsonData.put("displaySeq", 30);
		m.create("crud.jsoncrud_cfg_values", jsonData);
		//
		jsonData.put("key", "testkey04_");
		jsonData.put("value", JSONObject.NULL);
		jsonData.put("displaySeq", 7);
		m.create("crud.jsoncrud_cfg_values", jsonData);
		//
		jsonData.put("key", "testkey05_");
		jsonData.put("value", "testvalue005");
		jsonData.put("enabled", false);
		m.create("crud.jsoncrud_cfg_values", jsonData);
		//		
		System.out.println("	1.1 C:"+jsonResult);

		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg", jsonWhere);
		System.out.println("	1.2 R:"+jsonArrResult);

		jsonData = new JSONObject();
		jsonData.put("enabled", true);
		jsonArrResult = m.update("crud.jsoncrud_cfg", jsonData, jsonWhere);
		System.out.println("	1.3 U:"+jsonArrResult);
		
		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("enabled", false);
		jsonArrResult = m.delete("crud.jsoncrud_cfg_values", jsonWhere);
		System.out.println("	1.4 D:"+jsonArrResult);
	}
	
	private void test2_SchemaValidation(CRUDMgr m) throws JsonCrudException
	{
		String s100 = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
		System.out.println();
		System.out.println("2. SchemaValidation");
		JSONObject jsonData = new JSONObject();
		jsonData.put("appNamespace", 1000);
		jsonData.put("moduleCode", s100+s100+s100);
		jsonData.put("enabled", "false");
		jsonData.put("createdTimestamp", "Jul 2017");
		
		Map<String, String[]> mapErr = m.validateDataWithSchema("crud.jsoncrud_cfg", jsonData, true);
		int i = 1;
		for(String sColName : mapErr.keySet())
		{
			String[] sErrors = mapErr.get(sColName);
			System.out.println("	"+(++i)+". '"+sColName+"' : ");
			int j = 1;
			for(String sErr : sErrors)
			{
				System.out.println("        	"+i+"."+(j++)+". "+sErr);
			}
		}
		
	}
	
	private void test3_CustomSQL(CRUDMgr m) throws JsonCrudException
	{
		StringBuffer sbSQL = new StringBuffer();
		sbSQL.append(" SELECT cfg.*, count(val.cfg_key) as totalKeys ");
		sbSQL.append(" FROM jsoncrud_cfg cfg, jsoncrud_cfg_values val ");
		sbSQL.append(" WHERE val.cfg_id = cfg.cfg_id GROUP BY cfg.cfg_id ");
		
		System.out.println();
		System.out.println("3. Custom SQL");
		JSONObject jsonResult = m.retrieve("crud.jsoncrud_cfg", sbSQL.toString(), null, 0, 0);
		
		System.out.println("	3.1 Count & Group By "+jsonResult);
		
	}
	
	private void test4_Sorting_Returns(CRUDMgr m) throws JsonCrudException
	{
		String[] sSorting = new String[]{"displaySeq.desc", "cfgId"};
		String[] sReturns = new String[]{"displaySeq", "cfgId", "enabled","key", "value"};
		
		long id = getCfgId(m);
		
		System.out.println();
		System.out.println("4. Test Sorting, Returns");
		JSONArray jsonArrResult = null;
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("value.contain", "value");
		jsonWhere.put("displaySeq.from", 0);
		jsonWhere.put("displaySeq.to", 2000);
		jsonWhere.put("key.startwith", "test");
		jsonWhere.put("key.endwith", "_");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", jsonWhere);
		
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", 
				jsonWhere, sSorting, sReturns);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("	"+jsonArrResult.getJSONObject(i));
		}
		
	}
	
	private long getCfgId(CRUDMgr m) throws JsonCrudException
	{
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("appNamespace", "jsoncrud-framework");
		jsonWhere.put("moduleCode", "unit-test");
		
		JSONArray jArrResult = m.retrieve("crud.jsoncrud_cfg", jsonWhere);
		JSONObject jsonResult = jArrResult.getJSONObject(0);
		return jsonResult.getLong("cfgId");
	}
	
	public static void main(String args[]) throws Exception
	{
		CRUDMgrTest test = new CRUDMgrTest();
		long lStart = System.currentTimeMillis();
		
		CRUDMgr m = new CRUDMgr();
		Random random = new Random(lStart);

		try{
			Map<String, String> map = m.getAllConfig();
			for(String sKey : map.keySet())
			{
				String sVal = map.get(sKey);
				System.out.println("[init] config - "+sKey+" = "+sVal);
			}

			System.out.println();
			System.out.println("0. clean up data");
			System.out.print("	0.1 delete.jsoncrud_cfg_values:");
			JSONArray jArr = m.delete("crud.jsoncrud_cfg_values", new JSONObject());
			if(jArr==null) jArr = new JSONArray();
			System.out.println(jArr.length());
			
			System.out.print("	0.2 delete.jsoncrud_cfg:");
			jArr = m.delete("crud.jsoncrud_cfg", new JSONObject());
			if(jArr==null) jArr = new JSONArray();
			System.out.println(jArr.length());
			//
			test.test1_CRUD(m);
			//
			test.test2_SchemaValidation(m);
			//
			test.test3_CustomSQL(m);
			//
			test.test4_Sorting_Returns(m);
			//////////////////////////


			//////////////////////////

			
			
		}
		finally
		{
			/*
			System.out.println("delete.sample_userroles:"+m.delete("crud.sample_userroles", new JSONObject()));
			System.out.println("delete.sample_users:"+m.delete("crud.sample_users", new JSONObject()));
			System.out.println("delete.sample_roles:"+m.delete("crud.sample_roles", new JSONObject()));
			*/
			
		}

		System.out.println();
		System.out.println("### elapse: "+(System.currentTimeMillis()-lStart)+" ms");
	}
}
