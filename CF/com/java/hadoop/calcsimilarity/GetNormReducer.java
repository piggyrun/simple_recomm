package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetNormReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String uv = "";
		String sim_lists = "";
		for (Text val : values)
		{
			String v = val.toString();
			if (v.startsWith("u:")) // uv
			{
				uv = v.substring(2);
			}
			else if (v.startsWith("i:")) // item
			{
				sim_lists += (v.substring(2) + "\t");
			}
		}
		String out = "";
		if (!uv.isEmpty() && sim_lists.length() > 0)
		{
			out = sim_lists.substring(0, sim_lists.length()-1);
			context.write(key, new Text(uv + "\t" + out));
		}
	}	
}