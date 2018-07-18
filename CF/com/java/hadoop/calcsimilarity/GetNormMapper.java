package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GetNormMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] item_weights = map_string.split("\t");
		if (item_weights.length == 2) // item's UV
		{
			String item = item_weights[0];
			String uv = item_weights[1];
			context.write(new Text(item), new Text("u:" + uv));
		}
		else if (item_weights.length == 3) // weight between 2 items
		{
			context.write(new Text(item_weights[0]), new Text("i:" + item_weights[1] + "\t" + item_weights[2]));
		}
	}
}