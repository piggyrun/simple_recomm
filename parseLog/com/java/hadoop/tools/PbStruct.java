package com.java.hadoop.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class PbStruct {

	// vr特征
	public String query;
	public String type; // vrobj
	public String stype; // vrid
	public String pos; // vr排序位置
	public String name; // 实体名称
	
	public String get_para_value(String token)
	{
	    String[] segs = token.split("=");
	    if(segs.length > 1)
	        return segs[1];
	    else
	        return null;
	}
	
	public static String urlDecode(String s, String charset)
	{
		String line = "";
		try {
			line = URLDecoder.decode(s, charset);
		} catch (IllegalArgumentException ex) {
			line = "";
		} catch (UnsupportedEncodingException ex) {
			line = "";
		}
		return line;
	}
	
	public boolean parse_raw_log(String line)
	{
		String[] tokens = line.split(" ");
    	int total = 0;
	    if (tokens.length > 4)
	    {
	    	String[] pbAttrs = tokens[4].split("&");
	    	for (int i = 0; i < pbAttrs.length; i++)
	    	{
	    		if(pbAttrs[i].indexOf("query=") == 0)
	    		{
	    			total++;
	    			String q = get_para_value(pbAttrs[i]);
	    			if (q != null)
	    			{
		    			q = urlDecode(q, "gbk");
		    			q = urlDecode(q, "gbk");
	    			}
	    			this.query = q;
	    		}
	    		else if(pbAttrs[i].indexOf("type=") == 0)
	    		{
	    			total++;
	    			this.type = get_para_value(pbAttrs[i]);
	    		}
	    		else if(pbAttrs[i].indexOf("stype=") == 0)
	    		{
	    			total++;
	    			this.stype = get_para_value(pbAttrs[i]);
	    		}
	    		else if(pbAttrs[i].indexOf("pos=") == 0)
	    		{
	    			total++;
	    			this.pos = get_para_value(pbAttrs[i]);
	    		}
	    		else if(pbAttrs[i].indexOf("name=") == 0)
	    		{
	    			total++;
	    			String n = get_para_value(pbAttrs[i]);
	    			if (n != null)
	    			{
	    				n = urlDecode(n, "utf-8");
	    			}
	    			this.name = n;
	    		}   		
	    	}
	    }

	    return total==5;
	}
	
	public String toString()
	{
		return this.query + "\t" 
			 + this.type + "\t"
			 + this.stype + "\t"
			 + this.pos + "\t"
			 + this.name;
	}
}
