package com.java.hadoop.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class LogStruct {

	//日志来源
	public String logSource;	
	//用户特征
	public String ip;
	public String uid;
	public String pid;
	public String mobile;
	public String ua;
	public String wuid;
	public String mid;
	//Session id
	public String uuid;
	//session特征
	public String query;
	public String pageNum;
	public String pageSize;
	public String pageType;
	public String cost;
	public String backlist;
	public String status;
	public String totalNum;
	public String vr;
	public String vrdetail;
	public String urls;
	public String abtest;
	public String novel;
	public String ad;
	public String datetime;
	public String result;
	
	public String get_log_type(String line)
	{
	    int pos = line.indexOf("[XML][pv4st]\t");
	    if(pos >= 0)
	        return "UC";
	    pos = line.indexOf("[WEB][pv4st]\t");
	    if(pos >= 0)
	        return "WB";
	    return null;
	}
	public String get_para_value(String token)
	{
	    String[] segs = token.split(":");
	    if(segs.length > 1)
	        return segs[1];
	    else
	        return null;
	}
	public String get_urls_value(String token)
	{
		String[] segs = token.split(":");
		if(segs.length> 2)
		{
			return token.replace("urls:","");
		}
		else
			return null;
	}
	public String get_date_value(String token)
	{
		String[] segs = token.split("\\]\\[");
	    if(segs.length > 1)
	        return segs[1];
	    else
	        return null;
	}
	public String get_date_time(String token)
	{
		String[] segs = token.split(":");
		if(segs.length> 2)
		{
			return token.replace("datetime:","");
		}
		else
			return null;
	}
	public static String urlDecode(String s, String charset) {

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
	public String parse_raw_log(String line)
	{
	    String log_type = get_log_type(line);
	    if(log_type == null)
	    	return null;
	    if(log_type.equals("WB") || log_type.equals("UC"))
	    {
	    	this.logSource = log_type;
	    	String[] tokens = line.split("\t");
	    	for(int i = 0; i < tokens.length; i++)
	    	{
	    		if(tokens[i].indexOf("INFO") >= 0)
	    		{   
	    			this.datetime = get_date_value(tokens[i]);		
	    		}
	    		else if(tokens[i].indexOf("uuid:") == 0)
	    		{
	    			this.uuid = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("query:") == 0)
	    		{
	    			String q = get_para_value(tokens[i]);
	    			this.query = q; //urlDecode(q, "UTF-8");
	    		}
	    		else if(tokens[i].indexOf("urls:") == 0)
	    		{
	    			this.urls = get_urls_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("vrdetail:") == 0)
	    		{
	    			this.vrdetail = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("pageSize:") == 0)
	    		{
	    			this.pageSize = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("pagenum:") == 0)
	    		{
	    			this.pageNum = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("pid:") == 0)
	    		{
	    			this.pid = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("cost:") == 0)
	    		{
	    			this.cost = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("backlist:") == 0)
	    		{
	    			this.backlist = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("status:") == 0)
	    		{
	    			this.status = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("vr:") == 0)
	    		{
	    			this.vr = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("ad:") == 0)
	    		{
	    			this.ad = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("totalNum:") == 0)
	    		{
	    			this.totalNum = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("abtest:") == 0)
	    		{
	    			this.abtest = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("novel:") == 0)
	    		{
	    			this.novel = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("pageType:") == 0)
	    		{
	    			this.pageType = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("uid:") == 0)
	    		{
	    			this.uid = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("wuid:") == 0)
	    		{
	    			this.wuid = get_para_value(tokens[i]);
	    		}
	    		else if(tokens[i].indexOf("mid:") == 0)
	    		{
	    			this.mid = get_para_value(tokens[i]);
	    		}
	    	}
	    }
	    if(this.uuid == null || this.uuid.equalsIgnoreCase("null") || this.uuid.length() == 0)
	    	return null;
	    return this.logSource;
	}
	
	public String toString()
	{
		return "uuid:" + this.uuid + "\t" 
				+ "logSource:" + this.logSource + "\t"
				+ "query:" + this.query + "\t"
				+ "urls:" + this.urls + "\t"
				+ "pageSize:" + this.pageSize + "\t"
				+ "pageNum:" + this.pageNum + "\t"
				+ "pid:" + this.pid + "\t"
				+ "cost:" + this.cost + "\t"
				+ "backlist:" + this.backlist + "\t"
				+ "status:" + this.status + "\t"
				+ "vr:" + this.vr + "\t"
				+ "ad:" + this.ad + "\t"
				+ "totalNum:" + this.totalNum + "\t"
				+ "abtest:" + this.abtest + "\t"
				+ "novel:" + this.novel  + "\t"
				+ "datetime:" + this.datetime  + "\t"
				+ "pageType:" + this.pageType + "\t"
				+ "wuid:" + this.wuid + "\t"
				+ "uid:" + this.uid + "\t"
				+ "mid:" + this.mid;
	}
	public String getUserInfoString()
	{
		String ret = null;
		if (this.vr != null && this.urls != null && this.vrdetail != null)
		{
			ret = this.wuid + "\t" + this.query;
			String[] url_list = this.urls.split("\3");
			
			// extract vr key field
			Map<String, String> vr_key_map = new HashMap<String, String>();
			String[] vrdetails = this.vrdetail.split("-");
			for (int i = 0; i < vrdetails.length; i++)
			{
				String[] infos = vrdetails[i].split("_");
				String vrid = "";
				for (int j = 0; j < infos.length; j++)
				{
					if (j == 0)
					{
						vrid = infos[0];
					}
					else if (infos[j].startsWith("k#"))
					{
						vr_key_map.put(vrid, infos[j].substring(2));
					}
				}
			}
			
			// output vr info
			for (int i = 0; i < url_list.length; i++)
			{
				if (url_list[i].startsWith("vr_"))
				{
					String[] vr_info = url_list[i].split("#");
					if (vr_info.length > 2)
					{
						String vrid = vr_info[0].split("_")[1];
						String key = vr_key_map.get(vrid);
						if (key == null)
							key = this.query;
						String info = vr_info[0] + "_" + i + "_" + key + "_" + vr_info[2].substring(2);
						ret += ("\t" + info);
					}
				}
			}
		}
		return ret;
	}
	/*public static void main(String[] args)
	{
		String line = "uuid:00002a68-39fc-4802-90a1-5553ae81a2b3	logSource:UC	query:%E7%A9%BF%E8%B6%8A%E5%B0%8F%E8%AF%B4%E5%90%A7	urls:vr_30000701_s#pc_tc#0_http://www.sj131.com/web_s#pc_tc#0_http://www.qbxs8.com/web_s#pc_tc#0_http://www.sj131.com/modules/article/index.php?fullflag=1web_s#insert_tc#0_http://wap.xs8.cn/web_s#pc_tc#0_http://www.chuanyuemi.com/web_s#pc_tc#0_http://www.xs8.cn/chuanyue.htmlvr_30000401_s#pc_tc#0_http://tieba.baidu.com/f?kw=%B4%A9%D4%BD%D0%A1%CB%B5vr_11000301_s#wapvr_tc#0_http://wap.sogou.com/book/searchList.jsp?keyword=%E7%A9%BF%E8%B6%8A&titleOnly=on&e=1023vr_30000401_s#pc_tc#0_http://tieba.baidu.com/f?kw=%B4%A9%D4%BD%D0%A1%CB%B5%B0%C9vr_30000101_s#web2wap_tc#0_http://wapbaike.baidu.com/view/8084803.htm?ssid=0	pageSize:10	pageNum:1	pid:ucwebxml	cost:99	backlist:127	status:1	vr:30000701_1-30000401_7-11000301_8-30000401_9-30000101_10	ad:0	totalNum:2349840	abtest:A	novel:3	datetime:2013-10-24 21:06:16,861	pageType:2	uid:485538d967a07419cd15e1d589a245d6	mid:null";
		LogStruct lg = new LogStruct();
		lg.parse(line);
	}*/
}
