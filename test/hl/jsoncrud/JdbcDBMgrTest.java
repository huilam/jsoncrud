package hl.jsoncrud;

import java.sql.Connection;
import hl.common.JdbcDBMgr;
import hl.jsoncrud.CRUDMgr;

public class JdbcDBMgrTest {
	
	public static void main(String args[]) throws Exception
	{
		
		/////
		CRUDMgr m = new CRUDMgr();
		JdbcDBMgr jdbcMgr = m.getJdbcMgr("jdbc.postgres");
		Connection conn = null;
		
		int iTotalConn = 10;
		System.out.println("1. Test "+iTotalConn+" connections.");
		for(int i=0; i<iTotalConn; i++)
		{
			conn = jdbcMgr.getConnection();
//			System.out.println("		- "+(i+1)+"- isClosed():"+conn.isClosed()+", isValid(1):"+conn.isValid(1));
//			jdbcMgr.closeQuietly(conn, null, null);
		}
		System.out.println("		    - TotalConnInUse:"+jdbcMgr.getTotalConnInUse());
		jdbcMgr.closeAllConnInUse();
		System.out.println("		    - TotalConnInUse (after close all):"+jdbcMgr.getTotalConnInUse());
		/////
		System.out.println("2. Test closed connection.");
		conn = jdbcMgr.getConnection();
		conn.close();
		System.out.println("		- isClosed():"+conn.isClosed()+", isValid(1):"+conn.isValid(1));
	}
}
