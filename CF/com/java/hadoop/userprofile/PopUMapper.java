package com.java.hadoop.userprofile;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PopUMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] user_profile = map_string.split("\t");
		if (user_profile.length > 2)
		{
			String wuid = user_profile[0];
			String cnt = user_profile[1];
			context.write(new Text(cnt + "\t" + wuid), new Text(""));
		}
	}
}