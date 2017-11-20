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

import hl.jsoncrud.CRUDMgr;

public class CRUDMgrTest {
	
	public static void main(String args[]) throws Exception
	{
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
			
			System.out.println("1. delete.sample_user_attrs:"+m.delete("crud.sample_userattrs", new JSONObject()));
			System.out.println("2. delete.sample_userroles:"+m.delete("crud.sample_userroles", new JSONObject()));
			System.out.println("3. delete.sample_users:"+m.delete("crud.sample_users", new JSONObject()));
			System.out.println("4. delete.sample_roles:"+m.delete("crud.sample_roles", new JSONObject()));

			System.out.println();
			JSONObject jsonRole = new JSONObject();
			jsonRole.put("rolename", "dev");
			jsonRole.put("roledesc", "java developer");
			jsonRole = m.create("crud.sample_roles", jsonRole);
			System.out.println("5. create.sample_roles:"+jsonRole);
			
			jsonRole = new JSONObject();
			jsonRole.put("rolename", "admin");
			jsonRole.put("roledesc", "administrator");
			jsonRole = m.create("crud.sample_roles", jsonRole);
			System.out.println("6. create.sample_roles:"+jsonRole);
			
			JSONObject jsonUser = new JSONObject();
			jsonUser.put("uid", "huilam_ong");
			jsonUser.put("displayname", "Hui Lam");
			jsonUser.put("gender", "M");
			jsonUser.put("enabled", true);
			jsonUser.put("age", 100);
			
			JSONObject jsonUserAttrs = new JSONObject();
			jsonUserAttrs.put("race","human being");
			jsonUserAttrs.put("height","1cm");
			jsonUser.put("attrs", jsonUserAttrs);
			
			jsonUser = m.create("crud.sample_users", jsonUser);
			System.out.println("7. create.sample_users:"+jsonUser);
	
			JSONObject jsonWhere = new JSONObject();
			jsonWhere.put("uid", "huilam_ong");
			jsonUser = new JSONObject();
			jsonUser.put("displayname", "HL");
			jsonUser.put("age", 999);
			
			jsonUserAttrs = new JSONObject();
			jsonUserAttrs.put("height","1cm");
			jsonUserAttrs.put("company","nls");
			jsonUser.put("attrs", jsonUserAttrs);
			
			JSONArray jArr = m.update("crud.sample_users", jsonUser, jsonWhere);
			for(int i=0; i<jArr.length(); i++)
			{
				System.out.println("8. update.sample_users:"+jArr.get(i));
			}
			
			jsonUser = new JSONObject();
			jsonUser.put("uid", "huilam_ong");
			jsonUser = m.retrieveFirst("crud.sample_users", jsonUser);
			//
			jsonRole = new JSONObject();
			jsonRole.put("rolename", "admin");
			jsonRole = m.retrieveFirst("crud.sample_roles", jsonRole);
			//
			JSONObject jsonUserRoles = new JSONObject();
			jsonUserRoles.put("userid", jsonUser.get("id"));
			jsonUserRoles.put("roleid", jsonRole.get("id"));
			jsonUserRoles = m.create("crud.sample_userroles", jsonUserRoles);
			System.out.println("9. create.sample_userroles:"+jsonUserRoles);
			
			jsonRole = new JSONObject();
			jsonRole.put("rolename", "dev");
			jsonRole = m.retrieveFirst("crud.sample_roles", jsonRole);
			
			jsonUserRoles = new JSONObject();
			jsonUserRoles.put("userid", jsonUser.get("id"));
			jsonUserRoles.put("roleid", jsonRole.get("id"));
			jsonUserRoles = m.create("crud.sample_userroles", jsonUserRoles);
			System.out.println("10. create.sample_userroles:"+jsonUserRoles);
			//
			
			jArr = m.retrieve("crud.sample_users", jsonUser);
			for(int i=0; i<jArr.length(); i++)
			{
				System.out.println("11. retrieve.sample_users:"+jArr.get(i));
			}
			
			/////////// Test Rollback
			try {
				jsonUser = new JSONObject();
				jsonUser.put("uid", "rollback_uid");
				jsonUser.put("displayname", "Rollback User Name");
				jsonUser.put("gender", "M");
				jsonUser.put("enabled", true);
				jsonUser.put("age", 1);
				
				jsonUserAttrs = new JSONObject();
				jsonUserAttrs.put("1234567890123456789012345678901234567890123456789012345678901234567890","");
				jsonUser.put("attrs", jsonUserAttrs);
			
				jsonUser = m.create("crud.sample_users", jsonUser);
			}catch(Exception ex) {
			}
			
			jsonUser = new JSONObject(); 
			jsonUser.put("uid", "rollback_uid");
			jsonUser = m.retrieveFirst("crud.sample_users", jsonUser);
			System.out.println("12. rollback.create.sample_users (should be null):"+jsonUser);
			//////////////////////////			
			// Range filter test
			
			for(int i=0; i<100; i++)
			{
				jsonUser = new JSONObject();
				jsonUser.put("uid", "uid_"+i);
				jsonUser.put("displayname", "name_"+i);
				jsonUser.put("age", i);
				jsonUser = m.create("crud.sample_users", jsonUser);
			}
			
			jsonUser = new JSONObject();
			jsonUser.put("age.from", 5);
			jsonUser.put("age.to", 9);
			jsonUser.put("age.not", 8);
			jsonUser.put("uid.from", "uid_6");
			jArr = m.retrieve("crud.sample_users", jsonUser);
			System.out.println("13. retrieve.sample_users (age>=5 + age<=9 + uid>='uid_6'):"+jArr.length());
			for(int i=0; i<jArr.length(); i++)
			{
				System.out.println("    13."+(i+1)+" - "+jArr.get(i));
			}

			/////
			jsonUser = new JSONObject();
			jsonUser.put("displayname.ci.not", "NAME_32");
			jsonUser.put("displayname.ci", "NAME_*");
			jsonUser.put("uid.startwith.ci", "uiD_");
			jsonUser.put("uid.endwith", "2");
			jArr = m.retrieve("crud.sample_users", jsonUser);
			System.out.println("14. retrieve.sample_users (displayname='*5*' + uid.startwith='uid_' + uin.endwith='2'):"+jArr.length());
			for(int i=0; i<jArr.length(); i++)
			{
				System.out.println("    14."+(i+1)+" - "+jArr.get(i));
			}
			
			//////////////////////////
			JSONObject json = m.retrieve("crud.sample_users", new JSONObject(), 0, 3, new String[] {"id"}, false );
			System.out.println("15. Pagination");
			System.out.println("  - "+m._LIST_META+" = "+json.get(m._LIST_META));
			
			JSONArray jarr = json.getJSONArray(m._LIST_RESULT);
			System.out.println("  - "+m._LIST_RESULT+" = "+jarr.length());
			for(int i=0; i<jarr.length(); i++)
			{
				System.out.println("       15."+(i+1)+" - "+jarr.getJSONObject(i).toString());
			}
			
			//////////////////////////
			System.out.println();
			jsonRole = new JSONObject();
			String s100 = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
			jsonRole.put("id", 1000);
			jsonRole.put("rolename", "");
			jsonRole.put("roledesc", s100+s100+s100+s100+s100);
			jsonRole.put("createdTimestamp", "Jul 2017");
			
			Map<String, String> mapErr = m.validateDataWithSchema("crud.sample_roles", jsonRole, true);
			System.out.println("16. validation errors = "+mapErr.size());
			for(String sColName : mapErr.keySet())
			{
				System.out.println("  - "+sColName+" : "+mapErr.get(sColName));
			}
			
			//////////////////////////
			json = m.retrieve("crud.sample_users", 
					" SELECT u.*, count(a.attrkey) as totalAttrs "+
					" FROM jsoncrud_sample_users u, jsoncrud_sample_user_attrs a "+
					" WHERE a.user_id = u.id GROUP BY u.id", 
					null, 0 ,3);
			System.out.println("17. Custom SQL");
			System.out.println("  - "+m._LIST_META+" = "+json.get(m._LIST_META));
			
			jarr = json.getJSONArray(m._LIST_RESULT);
			System.out.println("  - "+m._LIST_RESULT+" = "+jarr.length());
			for(int i=0; i<jarr.length(); i++)
			{
				System.out.println("       17."+(i+1)+" - "+jarr.getJSONObject(i).toString());
			}
			
			json = m.retrieve("crud.sample_users", 
					" select currval('jsoncrud_sample_users_id_seq') ", 
					null, 0 ,0);
			System.out.println("18. Get Current Sequence with SQL");
			System.out.println("  - "+m._LIST_META+" = "+json.get(m._LIST_META));
			
			jarr = json.getJSONArray(m._LIST_RESULT);
			System.out.println("  - "+m._LIST_RESULT+" = "+jarr.length());
			for(int i=0; i<jarr.length(); i++)
			{
				System.out.println("       18."+(i+1)+" - "+jarr.getJSONObject(i).toString());
			}
			
			//////////////////////////
		/**
			jsonWhere = new JSONObject();
			jsonUser = new JSONObject();
			jsonUser.put("uid", "huilam_ong");
			jsonUser = m.retrieveFirst("crud.sample_users", jsonUser);
			jsonWhere.put("uid", jsonUser.getString("uid"));
			jsonUserAttrs = new JSONObject();
			jsonUserAttrs.put("race", "alien"); //update existing
			jsonUserAttrs.put("company", "nls"); //remain existing
			jsonUserAttrs.put("weight", "1kg"); //adding new attribute
			jsonUserAttrs.put("height", ""); //try to delete
			jsonUser.put("attrs", jsonUserAttrs);
			
			//
			jsonUser.remove("roles");
			
			jarr = m.update("crud.sample_users", jsonUser, jsonWhere);
			System.out.println("18. Update child record keyvalue pair :"+jarr.getJSONObject(0));
		**/
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
