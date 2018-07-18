package com.java.hadoop.calcrecomm;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RecommUserMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		int idx = map_string.indexOf("\t");
		if (idx >= 0)
		{
			String wuid = map_string.substring(0, idx);
			String val = map_string.substring(idx+1);
			context.write(new Text(wuid), new Text(val));
		}
	}
}