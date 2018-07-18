package com.java.hadoop.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		int pv = 0;
		Map<String, Integer> item_cnt_map = new HashMap<String, Integer>();
		for (Text val : values) 
		{
			if (val.toString().equals("pv"))
			{
				pv++;
			}
			else
			{
				if (item_cnt_map.containsKey(val.toString()))
				{
					int cnt = item_cnt_map.get(val.toString());
					cnt++;
					item_cnt_map.put(val.toString(), cnt);
				}
				else
				{
					item_cnt_map.put(val.toString(), 1);
				}					
			}
		}
		if (item_cnt_map.isEmpty())
			return;
		
		String lists = pv + "";
		for (String item : item_cnt_map.keySet()) // not record the count of each items
		{
			lists += ("\t" + item);
		}
		context.write(key, new Text(lists));
	}	
}