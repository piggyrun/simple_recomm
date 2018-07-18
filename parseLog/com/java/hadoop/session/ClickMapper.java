package com.java.hadoop.session;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClickMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = new String(map_value.getBytes(), "gbk");
		String[] tokens = map_string.split("\t");
		if (tokens.length >= 8)
		{
			String state = tokens[1];
			String query = tokens[3];
			String click_url = tokens[6];
			if (!query.equals("-"))
			{
				if (state.equals("0")) // pv
				{
					context.write(new Text(query), new Text("pv"));
				}
				else if (state.equals("1")) // click
				{
					String site = getSite(click_url);
					if (site != null)
						context.write(new Text(query), new Text(site));
				}
			}
		}
	}
	
	public String getSite(String url)
	{
		String ret = null;
		if (url != null)
		{
			int pos = url.indexOf('/', 7);
			if (pos != -1)
			{
				ret = url.substring(0, pos+1);
			}
		}
		return ret;
	}
}