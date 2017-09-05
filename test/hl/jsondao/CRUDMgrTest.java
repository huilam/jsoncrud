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

package hl.jsondao;

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
			System.out.println("1. delete.sample_userroles:"+m.delete("crud.sample_userroles", new JSONObject()));
			System.out.println("2. delete.sample_users:"+m.delete("crud.sample_users", new JSONObject()));
			System.out.println("3. delete.sample_roles:"+m.delete("crud.sample_roles", new JSONObject()));

			System.out.println();
			JSONObject jsonRole = new JSONObject();
			jsonRole.put("rolename", "dev");
			jsonRole.put("roledesc", "java developer");
			jsonRole = m.create("crud.sample_roles", jsonRole);
			System.out.println("4. create.sample_roles:"+jsonRole);
			
			jsonRole = new JSONObject();
			jsonRole.put("rolename", "admin");
			jsonRole.put("roledesc", "administrator");
			jsonRole = m.create("crud.sample_roles", jsonRole);
			System.out.println("5. create.sample_roles:"+jsonRole);
			
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
			System.out.println("6. create.sample_users:"+jsonUser);
	
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
				System.out.println("7. update.sample_users:"+jArr.get(i));
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
			System.out.println("8. create.sample_userroles:"+jsonUserRoles);
			
			jsonRole = new JSONObject();
			jsonRole.put("rolename", "dev");
			jsonRole = m.retrieveFirst("crud.sample_roles", jsonRole);
			
			jsonUserRoles = new JSONObject();
			jsonUserRoles.put("userid", jsonUser.get("id"));
			jsonUserRoles.put("roleid", jsonRole.get("id"));
			jsonUserRoles = m.create("crud.sample_userroles", jsonUserRoles);
			System.out.println("9. create.sample_userroles:"+jsonUserRoles);
			//
			
			jArr = m.retrieve("crud.sample_users", jsonUser);
			for(int i=0; i<jArr.length(); i++)
			{
				System.out.println("10. retrieve.sample_users:"+jArr.get(i));
			}
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
