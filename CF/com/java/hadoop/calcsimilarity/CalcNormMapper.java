package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CalcNormMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] item_list = map_string.split("\t");
		if (item_list.length >= 4) // item uv item1 occ1 [item2 occ2 ...]
		{
			String item = item_list[0];
			String uv = item_list[1];
			for (int i = 2; i+1 < item_list.length; i+=2)
			{
				String itemi = item_list[i];
				String occi = item_list[i+1];
				context.write(new Text(item + "\t" + itemi), new Text(occi + "\t" + uv));
				context.write(new Text(itemi + "\t" + item), new Text(occi + "\t" + uv));
			}
			
		}
	}
}