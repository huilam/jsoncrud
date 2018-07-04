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

public class JsonCrudException extends Exception {

	private static final long serialVersionUID = -1068899117288657750L;
	private String error_code 	= null;
	private String error_msg 	= null;	
	
	private Throwable throwable 	= null;
	private String error_debuginfo 	= null;
	private String error_cause 		= null;
	private boolean debug_mode 		= false;

	public JsonCrudException(String aErrCode, String aErrMessage)
	{
		super(aErrMessage);
		init(aErrCode, aErrMessage, null, null);
	}
	
	public JsonCrudException(String aErrCode, String aErrMessage, Throwable aThrowable)
	{
		super(aErrMessage, aThrowable);
		init(aErrCode, aErrMessage, null, aThrowable);
	}
	
	public JsonCrudException(String aErrCode, String aErrMessage, String aErrDebugInfo, Throwable aThrowable)
	{
		super(aErrMessage, aThrowable);
		init(aErrCode, aErrMessage, aErrDebugInfo, aThrowable);
	}
	
	
	public JsonCrudException(String aErrCode, Throwable aThrowable)
	{
		super(aThrowable);
		init(aErrCode, null, null, aThrowable);
	}
	
	private void init(String aErrCode, String aErrMessage, String aErrDebugInfo, Throwable aThrowable)
	{
		error_code = aErrCode;
		error_cause = getCauseErrMsg(aThrowable);
		error_msg = aErrMessage;
		error_debuginfo = aErrDebugInfo;
		throwable = aThrowable;
		
		if(error_msg==null)
		{
			error_msg = error_cause;
			error_cause = null;
		}
		
		if(aThrowable!=null)
		{
			aThrowable.printStackTrace();
		}
	}

	
	public String getErrorCode()
	{
		return error_code;
	}
	
	public String getErrorMsg()
	{
		return error_msg;
	}
	
	public Throwable getThrowable()
	{
		return throwable;
	}
	
	public String getErrorCause()
	{
		return error_cause;
	}
	
	public String getErrorDebugInfo()
	{
		return error_debuginfo;
	}
	
	public void setErrorMsg(String aErrorMsg)
	{
		error_msg = aErrorMsg;
	}
	
	public void setErrorCause(String aErrorCause)
	{
		error_cause = aErrorCause;
	}
	
	public void setErrorDebugInfo(String aErrorDebugInfo)
	{
		error_debuginfo = aErrorDebugInfo;
	}
	
	public void setDebugMode(boolean isDebug)
	{
		debug_mode = isDebug;
	}

	public String getMessage()
	{
		String sErrMsg = getErrorMsg();
		String sErrCause = getErrorCause();
		
		StringBuffer sbErr = new StringBuffer();
		sbErr.append(getErrorCode());
		sbErr.append(":").append(sErrMsg);
		//
		if(sErrCause!=null)
		{
			sbErr.append(" (").append(sErrCause).append(")");
		}
		//
		if(debug_mode)
		{
			sbErr.append("\n").append(getErrorDebugInfo());
		}
		
		return sbErr.toString();
	}
	
	private String getCauseErrMsg(Throwable aThrowable)
	{
		if(aThrowable==null)
			return null;
		
		if(aThrowable instanceof JsonCrudException)
		{
			return ((JsonCrudException)aThrowable).error_cause;
		}
		
		Throwable t = aThrowable.getCause();
		if(t!=null && t.getMessage()!=null)
		{
			return  t.getMessage();
		}
		
		return aThrowable.getMessage();
	}
}