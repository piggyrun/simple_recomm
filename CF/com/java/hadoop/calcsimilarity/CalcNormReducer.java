package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcNormReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String v = "";
		for (Text val : values)
		{
			v += (val.toString() + "\t");
		}
		String out = v.substring(0, v.length()-1);
		context.write(key, new Text(out));
	}	
}