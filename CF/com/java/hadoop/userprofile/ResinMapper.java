package com.java.hadoop.userprofile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ResinMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] user_infos = map_string.split("\t");
		if (user_infos.length < 3)
			return;
		String wuid = user_infos[0];
		String query = urlDecode(user_infos[1], "utf-8");
		
		for (int i = 2; i < user_infos.length; i++)
		{
			if (user_infos[i].startsWith("vr_"))
			{
				String[] results = user_infos[i].split("_");
				if (results.length >= 6)
				{
					if (results[1].equals("11000301")) // novel VR
					{
						String key = urlDecode(results[4], "utf-8");
						context.write(new Text("nv:" + key + "_1"), new Text("nv:" + wuid));
					}
					else if (results[1].equals("11000203") || results[1].equals("11001101")
						  || results[1].equals("40009001") || results[1].equals("21135801")) // app VR
					{
						String key = urlDecode(results[4], "utf-8");
						context.write(new Text("ap:" + key + "_1"), new Text("ap:" + wuid));
					}
					else if (results[1].startsWith("700007") || results[1].startsWith("700072")
						  || results[1].startsWith("700071") || results[1].startsWith("700100")) // movie VR
					{
						String key = query;
						context.write(new Text("mv:" + key + "_1"), new Text("mv:" + wuid));
					}
					else if (results[1].startsWith("700005")) // TV VR
					{
						String key = query;
						context.write(new Text("tv:" + key + "_1"), new Text("tv:" + wuid));
					}
					else if (results[1].startsWith("700010") || results[1].startsWith("700047")) // comic VR
					{
						String key = query;
						context.write(new Text("cm:" + key + "_1"), new Text("cm:" + wuid));
					}
					else if (results[1].startsWith("700008")) // show VR
					{
						String key = query;
						context.write(new Text("sw:" + key + "_1"), new Text("sw:" + wuid));
					}
				}
			}
		}
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
}