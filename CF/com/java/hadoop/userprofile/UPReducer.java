package com.java.hadoop.userprofile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UPReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Map<String, Integer> item_cnt_map = new HashMap<String, Integer>();
		int user_pv = 0;
		for (Text val : values) 
		{
			user_pv++;
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
		String lists = user_pv + "";
		for (String item : item_cnt_map.keySet()) // not record the count of each items
		{
			lists += ("\t" + item);
		}
		context.write(key, new Text(lists));
	}	
}