package com.java.hadoop.userprofile;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UPMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] infos = map_string.split("\t");
		if (infos.length < 1)
			return;
		String item = infos[0];
		
		for (int i = 1; i < infos.length; i++)
		{
			// wuid --> item
			context.write(new Text(infos[i]), new Text(item));
		}
	}
}