package hl.jsoncrud;

import java.sql.Connection;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import hl.common.JdbcDBMgr;
import hl.jsoncrud.CRUDMgr;

public class JdbcDBMgrTest {
	
	public static void main(String args[]) throws Exception
	{
		
		/////
		CRUDMgr m = new CRUDMgr();
		JdbcDBMgr jdbcMgr = m.getJdbcMgr("jdbc.postgres");
		Connection conn = jdbcMgr.getConnection();
		
		for(int i=0; i<10; i++)
		{
			conn = jdbcMgr.getConnection();
			System.out.println((i+1)+"- conn.isClosed():"+conn.isClosed());
			jdbcMgr.closeQuietly(conn, null, null);
		}
		/////
	}
}
