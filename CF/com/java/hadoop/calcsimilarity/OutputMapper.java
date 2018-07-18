package com.java.hadoop.calcsimilarity;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class OutputMapper extends Mapper<Object, Text, Text, Text> 
{
	String is_debug;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		is_debug = context.getConfiguration().get("recomm.calcsimilarity.outdebug");
	}
	
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] user_infos = map_string.split("\t");
		if (user_infos.length == 6)
		{
			String item1 = user_infos[0];
			String item2 = user_infos[1];
			if (!is_debug.equals("true"))
			{
				item1 = urlDecode(user_infos[0], "UTF-8");
				item2 = urlDecode(user_infos[1], "UTF-8");
			}
			
			String wstr = user_infos[2]
					+ "\t" + user_infos[3]
					+ "\t" + user_infos[5];
			context.write(new Text(item1), new Text(item2 + "\t" + wstr));
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