package com.java.hadoop.calcsimilarity;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SimilarityMapper extends Mapper<Object, Text, Text, DoubleWritable> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] user_infos = map_string.split("\t");
		if (user_infos.length > 2)
		{
			int user_pv = Integer.parseInt(user_infos[1]);
			for (int i = 2; i < user_infos.length; i++)
			{
				context.write(new Text("u:" + user_infos[i]), new DoubleWritable(1.0));
				for (int j = i+1; j < user_infos.length; j++)
				{
					double weight = 1.0;
					context.write(new Text("p:" + user_infos[i] + "\t" + user_infos[j]), new DoubleWritable(weight));
					context.write(new Text("p:" + user_infos[j] + "\t" + user_infos[i]), new DoubleWritable(weight));
				}
			}
		}
	}
	
	public double calc_norm_weight(int user_pv)
	{
		double w = Math.log(1 + 1.0/user_pv);
		return w;
	}
}