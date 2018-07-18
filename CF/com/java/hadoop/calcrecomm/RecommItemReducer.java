package com.java.hadoop.calcrecomm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.java.hadoop.calcsimilarity.ItemSimilarInfo;

public class RecommItemReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		List<String> user_list = new ArrayList<String>();
		List<ItemSimilarInfo> sim_list = new ArrayList<ItemSimilarInfo>();
		for (Text val : values)
		{
			String v = val.toString();
			if (v.startsWith("u:")) // user list
			{
				user_list.add(v.substring(2));
			}
			else if (v.startsWith("i:")) // item list
			{
				ItemSimilarInfo info = new ItemSimilarInfo();
				info.getFromString(v.substring(2));
				sim_list.add(info);
			}
		}
		Collections.sort(sim_list); // descend sort by similarity
		
		for (int i = 0; i < user_list.size(); i++)
		{
			String uid = user_list.get(i);
			String items = "";
			for (int j = 0; j < sim_list.size() && j < 20; j++)
			{
				items += (sim_list.get(j).toString() + "\t");
			}
			if (!items.isEmpty())
			{
				String out = items.substring(0, items.length()-1);
				context.write(new Text(uid), new Text(out));
			}
		}
		
	}	
}