package com.java.hadoop.pv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PvReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		for (Text val : values) 
		{
			context.write(val, null); 
		}		
	}	
}