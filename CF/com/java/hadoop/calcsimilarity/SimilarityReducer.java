package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SimilarityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{
	MultipleOutputs<Text, DoubleWritable> mos;
	String uvPref = "";
	
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, DoubleWritable>(context);
		uvPref = context.getConfiguration().get("recomm.calcsimilarity.uvpref");
	}
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
	{
		String keystr = key.toString();
		if (keystr.startsWith("u:")) // item uv
		{
			String item = keystr.substring(2);
			double uv = 0;
			for (DoubleWritable val : values)
				uv += val.get();
			mos.write(new Text(item), new DoubleWritable(uv), uvPref);
		}
		else if (keystr.startsWith("p:")) // similarity between pairs
		{
			String item = keystr.substring(2); // item1"\t"item2
			double weight = 0;
			for (DoubleWritable val : values) 
				weight += val.get();
			context.write(new Text(item), new DoubleWritable(weight));
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		mos.close();
	}
}