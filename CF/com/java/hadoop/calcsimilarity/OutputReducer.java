package com.java.hadoop.calcsimilarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutputReducer extends Reducer<Text, Text, Text, Text> 
{
	String is_debug;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		is_debug = context.getConfiguration().get("recomm.calcsimilarity.outdebug");
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		List<ItemSimilarInfo> sim_list = new ArrayList<ItemSimilarInfo>();
		for (Text val : values)
		{
			ItemSimilarInfo info = new ItemSimilarInfo();
			info.getFromString(val.toString());
			sim_list.add(info);
		}
		Collections.sort(sim_list); // descend sort by similarity
		
		String out = "";
		for (ItemSimilarInfo info : sim_list)
		{
			if (info.weight > 0.001)
			{
				if (is_debug.equals("true"))
					out += (info.toStringDebug() + "\t");
				else
					out += (info.toString() + "\t");
			}
			else
			{
				break;
			}
		}
	
		if (!out.isEmpty())
			context.write(key, new Text(out));
	}
}