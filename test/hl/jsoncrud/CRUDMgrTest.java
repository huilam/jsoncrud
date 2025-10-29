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
import org.json.JSONArray;
import org.json.JSONObject;

public class CRUDMgrTest {
	
	private final static String _ATTRNAME_enabled 		= "debug.enabled";
	private final static String _ATTRNAME_displaySeq 	= "debug.displaySeq";
	 
	private void test1_CRUD(CRUDMgr m) throws JsonCrudException
	{
		String sTestData = "{\"moduleCode\": \"testpost180117-1\",\"kvpair\":" + 
				"{\"key003\": \"val003\",\"key002\": \"val002\",\"key001\": \"val001\" }," + 
				" \"appNamespace\": \"testpost180117-1\", \"no-such-dbfield\": \"test\"}";
		
		
		JSONObject jsonData 	= null;
		JSONObject jsonWhere 	= null;
		JSONObject jsonResult 	= null;
		JSONArray jsonArrResult = null;
		
		System.out.println();
		System.out.println("1. CRUD");
		jsonData = new JSONObject();
		jsonData.put("appNamespace", "jsoncrud-framework");
		jsonData.put("moduleCode", "unit-test-0");
		jsonData.put("enabled", true);
		//
		
		System.out.println("	1.1  Insert parent NO child");
		jsonResult = m.create("crud.jsoncrud_cfg", jsonData);
		System.out.println("		- "+jsonResult);		
		
		jsonResult = m.create("crud.jsoncrud_cfg", new JSONObject(sTestData));
		System.out.println("		- "+jsonResult);
		
		/////
		jsonData = new JSONObject();
		jsonData.put("appNamespace", "jsoncrud-framework");
		jsonData.put("moduleCode", "unit-test-1");
		jsonData.put("enabled", false);
		//
		JSONObject jsonDataChild = new JSONObject();
		jsonDataChild.put("testkey000_", "testvalue000_");
		jsonData.put("kvpair", jsonDataChild);
		//
		System.out.println("	1.2  Insert parent WITH child");
		jsonResult = m.create("crud.jsoncrud_cfg", jsonData);
		System.out.println("		- "+jsonResult);
		
		long id = jsonResult.getLong("cfgId");
		jsonData = new JSONObject();
		jsonData.put("cfgId", id);
		jsonData.put("key", "testvalue001_");
		jsonData.put("value", "testvalue001_");
		jsonData.put(_ATTRNAME_enabled, true);
		jsonData.put(_ATTRNAME_displaySeq, 1);
		System.out.println("	1.3  create childs");
		jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println(" 		- "+jsonResult);
		//
		jsonData.put("key", "%testkey02_");
		jsonData.put("value", "`testvalue|002_");
		jsonData.put("cfg_real", 0.6);
		jsonData.put(_ATTRNAME_displaySeq, 200);
		jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println(" 		- "+jsonResult);
		//
		jsonData.put("key", "testkey03_");
		jsonData.put("value", "testvalue003");
		jsonData.put(_ATTRNAME_displaySeq, 30);
		jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println(" 		- "+jsonResult);
		//
		jsonData.put("key", "testkey04_");
		jsonData.put("value", JSONObject.NULL);
		jsonData.put(_ATTRNAME_displaySeq, 7);
		jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println(" 		- "+jsonResult);
		//
		jsonData.put("key", "testkey05_");
		jsonData.put("value", "testvalue005");
		jsonData.put(_ATTRNAME_enabled, false);
		jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println(" 		- "+jsonResult);
		//		

		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg", jsonWhere);
		System.out.println("	1.4 R:"+jsonArrResult);

		jsonData = new JSONObject();
		jsonData.put(_ATTRNAME_enabled, true);
		jsonArrResult = m.update("crud.jsoncrud_cfg", jsonData, jsonWhere);
		System.out.println("	1.5 U:"+jsonArrResult);
		
		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put(_ATTRNAME_enabled, false);
		jsonArrResult = m.delete("crud.jsoncrud_cfg_values", jsonWhere);
		System.out.println("	1.6 D:"+jsonArrResult);
		
	}
	
	private void test2_SchemaValidation(CRUDMgr m) throws JsonCrudException
	{
		String s100 = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
		System.out.println();
		System.out.println("2. SchemaValidation");
		JSONObject jsonData = new JSONObject();
		jsonData.put("XXX", 1000);
		jsonData.put("cfgId", 1000);
		jsonData.put("appNamespace", 1000);
		jsonData.put("moduleCode", s100+s100+s100);
		jsonData.put("enabled", "false");
		jsonData.put("createdTimestamp", "Jul 2017");
		
		Map<String, String[]> mapErr = m.validateDataWithSchema("crud.jsoncrud_cfg", jsonData, true);
		int i = 0;
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
		System.out.println("	3.1 Count & Group By ");
		JSONObject jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg", sbSQL.toString(), null, 0, 0);
		System.out.println("		- "+jsonResult);
		
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("moduleCode.in", "unit-test-1");
		jsonWhere.put("appNamespace.endwith", "framework");
		System.out.println("	3.2 Count & Group By with filters ");
		jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg", 
				sbSQL.toString(), 
				jsonWhere, 0, 0, null, null);
		System.out.println("		- "+jsonResult);
		
		sbSQL.setLength(0);
		sbSQL.append(" SELECT cfg.*, val.* ");
		sbSQL.append(" FROM jsoncrud_cfg cfg LEFT OUTER JOIN jsoncrud_cfg_values val ON (val.cfg_id = cfg.cfg_id)");
		System.out.println("	3.4 Left Outer Join ");
		jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg", sbSQL.toString(), null, 0, 0, null, null);
		System.out.println("		- "+jsonResult);		
		
		
		System.out.println("	3.5 Sequence ");
		jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg", "select nextval(?)", new Object[]{"jsoncrud_cfg_cfg_id_seq"}, 0, 0);
		System.out.println("		- "+jsonResult);
		
		
		System.out.println("	3.6 Test Empty result SQL ");
		jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg", "select * from jsoncrud_cfg where 1=2", null, 0, 0);
		System.out.println("		- "+jsonResult);
	}
	
	private void test4_Sorting_Returns(CRUDMgr m) throws JsonCrudException
	{
		String[] sSorting = new String[]{_ATTRNAME_displaySeq+".desc", "cfgId", _ATTRNAME_enabled+".asc"};
		String[] sReturns = new String[]{_ATTRNAME_displaySeq, _ATTRNAME_enabled,"key"};
		
		long id = getCfgId(m);
		
		System.out.println();
		System.out.println("4. Test Filters, Sorting & Returns");
		JSONArray jsonArrResult = null;
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("value.startwith", "`");
		jsonWhere.put("value.contain", "|");
		jsonWhere.put(_ATTRNAME_displaySeq+".from", 20);
		jsonWhere.put(_ATTRNAME_displaySeq+".to", 1000);
		jsonWhere.put("key.startwith", "%");
		jsonWhere.put("key.endwith", "_");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", jsonWhere);
		
		System.out.println("	4.1 Filters : ");
		System.out.println("		4.1.1 \"value.contain\":\"|\" value.startwith\", \"`\"");
		System.out.println("		4.1.2 \""+_ATTRNAME_displaySeq+".from\":\"20\"  \""+_ATTRNAME_displaySeq+".to\":\"1000\"");	
		System.out.println("		4.1.3 \"key.startwith\":\"%\"  \"key.endwith\":\"_\"");
		
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", jsonWhere, null, null);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("			- "+jsonArrResult.getJSONObject(i));
		}
		
		System.out.println("	4.2 Sorting (\""+_ATTRNAME_displaySeq+".desc\", \"cfgId\") : ");
		
		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", 
				jsonWhere, sSorting, null);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}

		
		System.out.println("	4.3 Returns (\"displaySeq\", \"enabled\",\"key\") : ");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", 
				jsonWhere, null, sReturns);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		
		System.out.println("	4.4 Returns.exclude (\"emptysqlresult\", \"kvpair\", \"keys\") : ");		
		System.out.println("		All : ");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg", 
				jsonWhere, null, null, false);
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		
		sReturns = new String[]{"emptysqlresult", "kvpair"};
		System.out.println("		With returns : [emptysqlresult, kvpair]");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg", 
				jsonWhere, null, sReturns, false);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		sReturns = new String[]{"kvpair","keys","emptysqlresult"};
		System.out.println("		With returns.exclude : [kvpair, keys, emptysqlresult] ");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg", 
				jsonWhere, null, sReturns, true);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		
		
		
		
		
		
		
		System.out.println("	4.5  filter.in : ");

		System.out.println("		4.5.1 String : ");
		jsonWhere = new JSONObject();
		jsonWhere.put("value.in", "testvalue001_,testvalue000_");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", 
				jsonWhere, null, null);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		
		System.out.println("		4.5.2 Numberic : ");
		jsonWhere = new JSONObject();
		jsonWhere.put(_ATTRNAME_displaySeq+".in", "1 ,7");
		jsonArrResult = m.retrieve("crud.jsoncrud_cfg_values", 
				jsonWhere, null, null);
		
		for(int i=0; i<jsonArrResult.length(); i++)
		{
			System.out.println("		- "+jsonArrResult.getJSONObject(i));
		}
		
		
	}
	
	private void test5_Null(CRUDMgr m) throws JsonCrudException
	{
		long id = getCfgId(m);
		
		JSONObject jsonData = new JSONObject();
		jsonData.put("cfgId", id);
		jsonData.put("key", "testNULL");
		jsonData.put("value", JSONObject.NULL);

		System.out.println();	
		System.out.println("5. Test NULL value");
		System.out.println("	5.1 Insert NULL value");
		JSONObject jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println("		- "+jsonResult);
		//////
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("key", "testNULL");
		jsonData.put("value", JSONObject.NULL);
		
		jsonData = new JSONObject();
		jsonData.put("value", JSONObject.NULL);
		
		System.out.println("	5.2 Update NULL value to NULL value");
		JSONArray jArrResult = m.update("crud.jsoncrud_cfg_values", jsonData, jsonWhere);
		if(jArrResult==null)
			jArrResult = new JSONArray();
		for(int i=0; i<jArrResult.length(); i++)
		{
			System.out.println("		- "+jArrResult.getJSONObject(i));
		}		
		//////
		jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("key", "testNULL");
		jsonWhere.put("value", JSONObject.NULL);
		System.out.println("	5.3 Delete NULL value");
		jArrResult = m.delete("crud.jsoncrud_cfg_values", jsonWhere);
		if(jArrResult==null)
			jArrResult = new JSONArray();
		for(int i=0; i<jArrResult.length(); i++)
		{
			System.out.println("		- "+jArrResult.getJSONObject(i));
		}		
		//////
		jsonWhere = new JSONObject();
		jsonWhere.put("value", JSONObject.NULL);
		System.out.println("	5.4 Retrieve NULL value");
		jArrResult = m.retrieve("crud.jsoncrud_cfg_values", jsonWhere);
		if(jArrResult==null)
			jArrResult = new JSONArray();
		for(int i=0; i<jArrResult.length(); i++)
		{
			System.out.println("		- "+jArrResult.getJSONObject(i));
		}

		//////
		System.out.println("	5.5 Retrieve NULL SQL result ");
		String sSQL = "select cfg_key, cfg_value from jsoncrud_cfg_values where cfg_key = ?";
		jsonResult = m.retrieveBySQL("crud.jsoncrud_cfg_values", 
				sSQL, new Object[] {"not-suchrec-!#@#%!$#'"}, 0 ,0);
		System.out.println("		- "+jsonResult);
		
		//////
		
		jsonWhere = new JSONObject();
		jsonWhere.put("appNamespace", "jsoncrud-framework");
		jsonWhere.put("moduleCode", "unit-test-0");		
		System.out.println("	5.6 Retrieve record with NO sql mapping (values) result");
		jArrResult = m.retrieve("crud.jsoncrud_cfg", jsonWhere);
		if(jArrResult==null)
			jArrResult = new JSONArray();
		for(int i=0; i<jArrResult.length(); i++)
		{
			System.out.println("		- "+jArrResult.getJSONObject(i));
		}
	}
	
	private void test6_ExtraAttrs(CRUDMgr m) throws JsonCrudException
	{
		long id = getCfgId(m);
		
		JSONObject jsonData = new JSONObject();
		jsonData.put("cfgId", id);
		jsonData.put("key", "key12345");
		jsonData.put("value", 123456);
		jsonData.put("extra", "this is extra field");

		System.out.println();	
		System.out.println("6. Test Create with Extra attribute");
		JSONObject jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println("		- "+jsonResult);
		//////
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("key", "key12345");
				
		jsonData = new JSONObject();
		jsonData.put("value", 654321);
		jsonData.put("extra", "this is extra field");
		
		System.out.println("	6.2 Update with Extra attribute");
		JSONArray jArrResult = m.update("crud.jsoncrud_cfg_values", jsonData, jsonWhere);
		if(jArrResult==null)
			jArrResult = new JSONArray();
		for(int i=0; i<jArrResult.length(); i++)
		{
			System.out.println("		- "+jArrResult.getJSONObject(i));
		}		
		//////
	}

	private long getCfgId(CRUDMgr m) throws JsonCrudException
	{
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("appNamespace", "jsoncrud-framework");
		jsonWhere.put("moduleCode", "unit-test-1");
		
		JSONArray jArrResult = m.retrieve("crud.jsoncrud_cfg", jsonWhere);
		JSONObject jsonResult = jArrResult.getJSONObject(0);
		return jsonResult.getLong("cfgId");
	}
	
	public void testCustomSQLConfig(CRUDMgr m)
	{
		Map<String, String> mapConfig = m.getCrudConfigs("customsql");
		
		System.out.println();
		System.out.println(" Configuration listing :");
		
		int i=1;
		for(String sKey : mapConfig.keySet())
		{
			System.out.println(" - "+(i++)+". "+sKey+" : "+mapConfig.get(sKey));
		}
	}
	
	public void test7_UpdateToNull(CRUDMgr m) throws JsonCrudException
	{
		long id = getCfgId(m);
		
		JSONObject jsonData = new JSONObject();
		jsonData.put("cfgId", id);
		jsonData.put("key", "key999111");
		jsonData.put("value", 101010);

		System.out.println();	
		System.out.println("7.1 Create TestData");
		JSONObject jsonResult = m.create("crud.jsoncrud_cfg_values", jsonData);
		System.out.println("		- "+jsonResult);
		
		
		JSONObject jsonWhere = new JSONObject();
		jsonWhere.put("cfgId", id);
		jsonWhere.put("key", "key999111");
		//
		jsonData = new JSONObject();
		jsonData.put("value", JSONObject.NULL);
		
		System.out.println("7.2 Update value to NULL");
		JSONArray jArrResult = m.update("crud.jsoncrud_cfg_values", jsonData, jsonWhere);
		jsonResult = jArrResult.getJSONObject(0);
		System.out.println("		- "+jsonResult);
	}
	
	
	public static void main(String args[]) throws Exception
	{
		CRUDMgrTest test = new CRUDMgrTest();
		long lStart = System.currentTimeMillis();
		
		CRUDMgr m = new CRUDMgr();
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
			//
			test.test5_Null(m);
			//
			test.test6_ExtraAttrs(m);
			
			test.test7_UpdateToNull(m);

			//////////////////////////
			test.testCustomSQLConfig(m);

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
