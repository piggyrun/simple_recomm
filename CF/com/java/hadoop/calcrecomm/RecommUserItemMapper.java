package com.java.hadoop.calcrecomm;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommUserItemMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] infos = map_string.split("\t");
		if (infos.length > 2) // user pv item1 item2 ...
		{
			for (int i = 2; i < infos.length; i++)
			{
				context.write(new Text(infos[i]), new Text("u:" + infos[0])); // item --> user
			}
		}
	}
}