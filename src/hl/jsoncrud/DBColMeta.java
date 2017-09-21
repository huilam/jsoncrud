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

import java.sql.Types;

import org.json.JSONException;
import org.json.JSONObject;

public class DBColMeta extends JSONObject{

	public static String _TABLENAME 		= "tablename";
	public static String _COL_NAME 			= "col.name";
	public static String _COL_SIZE 			= "col.size";
	public static String _COL_SEQ 			= "col.seq";
	public static String _COL_TYPE 			= "col.type";
	public static String _COL_TYPENAME 		= "col.typename";
	public static String _COL_CLASSNAME 	= "col.classname";
	public static String _COL_NULLABLE 		= "col.nullable";
	public static String _COL_AUTOINCREMENT	= "col.autoIncrement";	
	
	public static String _JSONNAME			= "jsonname";
	
	
	//--
	public void setTablename(String aTablename)
	{
		set(_TABLENAME, aTablename);
	}
	//
	public String getTablename()
	{
		return get(_TABLENAME, null);
	}
	//--
	public void setJsonname(String aJsonname)
	{
		set(_JSONNAME, aJsonname);
	}
	//
	public String getJsonname()
	{
		return get(_JSONNAME, null);
	}
	//--
	public void setColname(String aColname)
	{
		set(_COL_NAME, aColname);
	}
	//
	public String getColname()
	{
		return get(_COL_NAME, null);
	}
	//--
	public void setColautoincrement(boolean aColAutoincrement)
	{
		set(_COL_AUTOINCREMENT, String.valueOf(aColAutoincrement));
	}
	//
	public boolean getColautoincrement()
	{
		return Boolean.parseBoolean(get(_COL_AUTOINCREMENT, "false"));
	}
	//--	
	public void setColtypename(String aColtypename)
	{
		set(_COL_TYPENAME, aColtypename);
	}
	//
	public String getColtypename()
	{
		return get(_COL_TYPENAME, null);
	}
	//--	
	public void setColclassname(String aColclassname)
	{
		set(_COL_CLASSNAME, aColclassname);
	}
	//
	public String getColclassname()
	{
		return get(_COL_CLASSNAME, null);
	}	
	//--	
	public void setColtype(String aColtype)
	{
		set(_COL_TYPE, aColtype);
	}
	//
	public String getColtype()
	{
		return get(_COL_TYPE, null);
	}
	//--		
	public void setColsize(int aColsize)
	{
		set(_COL_SIZE, String.valueOf(aColsize));
	}
	//
	public int getColsize()
	{
		return Integer.parseInt(get(_COL_SIZE, "0"));
	}
	//--	
	public void setColseq(int aColseq)
	{
		set(_COL_SEQ, String.valueOf(aColseq));
	}
	//
	public int getColseq()
	{
		return Integer.parseInt(get(_COL_SEQ, "0"));
	}	
	//--	
	public void setColnullable(boolean aColnullable)
	{
		set(_COL_NULLABLE, String.valueOf(aColnullable));
	}
	//
	public boolean getColnullable()
	{
		return Boolean.parseBoolean(get(_COL_NULLABLE, "true"));
	}
	//--	
	public boolean isBoolean()
	{
		String sColType = getColtype();
		if(sColType!=null)
		{
			int iColType = Integer.parseInt(sColType);
			switch(iColType)
			{
				case Types.BOOLEAN  :
					return true;
				default :
					return false;
			}
		}
		return false;		
	}
	
	public boolean isBit()
	{
		String sColType = getColtype();
		if(sColType!=null)
		{
			int iColType = Integer.parseInt(sColType);
			switch(iColType)
			{
				case Types.BIT  :
					return true;
				default :
					return false;
			}
		}
		return false;		
	}
	
	public boolean isString()
	{
		String sColType = getColtype();
		if(sColType!=null)
		{
			int iColType = Integer.parseInt(sColType);
			switch(iColType)
			{
				case Types.CHAR  :
				case Types.VARCHAR  :
				case Types.NVARCHAR  :
				case Types.LONGVARCHAR :
				case Types.LONGNVARCHAR :
					return true;
				default :
					return false;
			}
		}
		return false;		
	}

	public boolean isNumeric()
	{
		String sColType = getColtype();
		if(sColType!=null)
		{
			int iColType = Integer.parseInt(sColType);
			switch(iColType)
			{
				case Types.SMALLINT :
				case Types.BIGINT 	:
				case Types.TINYINT 	:
				case Types.INTEGER 	:
				case Types.DECIMAL 	:
				case Types.NUMERIC 	:

				case Types.DOUBLE 	:
				case Types.FLOAT 	:
					return true;
				default :
					return false;
			}
		}
		return false;
	}
	
	public boolean isTimestamp()
	{
		String sColType = getColtype();
		if(sColType!=null)
		{
			int iColType = Integer.parseInt(sColType);
			switch(iColType)
			{
				case Types.DATE 				:
				case Types.TIME 				:
				case Types.TIME_WITH_TIMEZONE 	:
				case Types.TIMESTAMP 			:
				case Types.TIMESTAMP_WITH_TIMEZONE 	:
					return true;
				default :
					return false;
			}
		}
		return false;
	}	
	//////
	private String get(String sKey, String sDefaulValue)
	{
		String sReturn = null;
		try{
			sReturn = (String) get(sKey);
		}catch(JSONException ex)
		{
			sReturn = sDefaulValue;
		}
		return sReturn;
	}
	
	private void set(String sKey, String sValue)
	{
		if(sValue!=null)
		{
			put(sKey, sValue);
		}
		else
		{
			try{
				remove(sKey);
			}catch(JSONException ex){}
		}
	}	
}
