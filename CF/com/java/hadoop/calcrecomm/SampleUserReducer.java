package com.java.hadoop.calcrecomm;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SampleUserReducer extends Reducer<Text, Text, Text, Text> 
{
	MultipleOutputs<Text, Text> mos;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		boolean is_out = false;
		String user_profile = "", user_results = "";
		for (Text val : values)
		{
			String v = val.toString();
			if (v.equals("s"))
			{
				is_out = true;
			}
			else if (v.startsWith("u:"))
			{
				user_profile = v.substring(2);
			}
			else if (v.startsWith("i:"))
			{
				user_results = v.substring(2);
			}
		}
		
		if (is_out)
		{
			context.write(new Text(user_results), null);
			mos.write(new Text(user_profile), null, "UserProfile");
		}
	}	
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		mos.close();
	}
}