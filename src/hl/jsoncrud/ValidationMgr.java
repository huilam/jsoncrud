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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidationMgr {
	//
	private class ValidationRule {
		//
		private String rule_name	= null;
		private String regex 		= null;
		private String err_code 	= null;
		private String err_msg 		= null;
		//
		public ValidationRule(String aRuleName, String aRegex)
		{
			setRule_name(aRuleName);
			setRegex(aRegex);
		}
		//
		public String getRule_name() {
			return rule_name;
		}
		public void setRule_name(String rule_name) {
			this.rule_name = rule_name;
		}
		//
		public String getRegex() {
			return regex;
		}
		public void setRegex(String regex) {
			//validate regex
			Pattern.compile(regex);
			this.regex = regex;
		}
		//
		public String getErr_code() {
			return err_code;
		}
		public void setErr_code(String err_code) {
			this.err_code = err_code;
		}
		//
		public String getErr_msg() {
			return err_msg;
		}
		public void setErr_msg(String err_msg) {
			this.err_msg = err_msg;
		}
		//
		public String toString()
		{
			StringBuffer sb = new StringBuffer();
			sb.append("rule_name=").append(getRule_name());
			sb.append(",regex=").append(getRegex());
			sb.append(",error_code=").append(getErr_code());
			sb.append(",error_msg=").append(getErr_msg());
			
			return sb.toString();
		}
		//
	}	
	
	public class Validation {
		//
		private Pattern pattValidation 	= null;
		//
		private ValidationRule rule		= null;
		private boolean validated_ok	= false;
		private String error_code 		= null;
		private String error_msg 		= null;
		private String input_text 		= null;
		//
		public Validation(ValidationRule aRule, String aInputString)
		{
			pattValidation = Pattern.compile(aRule.getRegex());
			this.rule = aRule;
			this.input_text = aInputString;
		}
		
		private void setError(String aErrCode, String aErrMsg)
		{
			this.error_code = aErrCode;
			this.error_msg 	= aErrMsg;
		}
		//
		public boolean isValidated_ok() {
			return this.validated_ok;
		}
		public String getValidation_rulename() {
			return this.rule.getRule_name();
		}
		public String getValidation_regex() {
			return this.rule.getRegex();
		}
		public String getInput_String() {
			return this.input_text;
		}
		public String getErr_msg() {
			return this.error_msg;
		}	
		public String getErr_code() {
			return this.error_code;
		}
		//
		public boolean validate()
		{
			Matcher m = pattValidation.matcher(getInput_String());
			if(m.find())
			{
				validated_ok = true;
			}
			else
			{
				validated_ok = false;
				setError(this.rule.getErr_code(), this.rule.getErr_msg());
			}
			return validated_ok;
		}
		
		public String toString()
		{
			StringBuffer sb = new StringBuffer();
			sb.append("ValidationRule=").append(rule);
			sb.append(",isValidatedOk=").append(isValidated_ok());
			sb.append(",input_string=").append(getInput_String());
			sb.append(",error_code=").append(getErr_code());
			sb.append(",error_msg=").append(getErr_msg());
			
			return sb.toString();
		}
		//
	}
	
	//
	public final static String _REGEX 		= "regex";
	public final static String _ERRCODE 	= "error.code";
	public final static String _ERRMSG 		= "error.message";
	
	private Map<String, ValidationRule> mapValidator = new HashMap<String, ValidationRule>();
	
	public ValidationMgr()
	{
		
	}
	
	protected ValidationRule addValitionRule(String aRuleName, String aRegex, String aErrCode, String aErrMsg) throws Exception
	{
		ValidationRule rule = null;
		
		if(aRuleName!=null && aRegex!=null)
		{
			rule = new ValidationRule(aRuleName, aRegex);
			rule.setErr_code(aErrCode);
			rule.setErr_msg(aErrMsg);
		}
		
		if(rule!=null)
		{
			ValidationRule ruleExisting = mapValidator.get(aRuleName);
			if(ruleExisting==null)
			{
				mapValidator.put(aRuleName, rule);
			}
			else
			{
				throw new Exception("Duplicate validationRule :"+
						"\n-[old] "+ruleExisting+
						"\n-[new] "+ruleExisting);
			}
		}
		
		return rule;
	}
	
	public Validation validate(String aValidationRuleName, String aTestString)
	{
		ValidationRule r = this.mapValidator.get(aValidationRuleName);
		if(r!=null)
		{
			Validation v = new Validation(r, aTestString);
			v.validate();
			return v;
		}
		return null;
	}
	
    public static void main(String args[]) throws Exception
    {
    	
    	ValidationMgr vMgr = new ValidationMgr();
    	vMgr.addValitionRule("NUMERIC_ONLY", "[0-9\\.]+", "ERR001", "Invalid format, value should be numberic.");
    	vMgr.addValitionRule("MASTERCODE", "[A-Za-z0-9_]+", "ERR002", "Invalid Mastercode format !");
    	
    	Validation v = vMgr.validate("MASTERCODE", "aaaaAA11245664");
    	System.out.println("testvalue : "+v.getInput_String());
    	System.out.println("rulename  : "+v.getValidation_rulename());
    	System.out.println("regex     : "+v.getValidation_regex());
    	System.out.println("result.OK : "+v.isValidated_ok());
    	if(!v.isValidated_ok())
    	{
    	System.out.println("error     : "+v.getErr_code()+" - "+v.getErr_msg());
    	}
    	System.out.println();
    	
    }

}
