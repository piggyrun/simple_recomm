package com.java.hadoop.calcrecomm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.java.hadoop.calcsimilarity.ItemSimilarInfo;

public class RecommUserReducer extends Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Map<String, ItemSimilarInfo> sim_map = new HashMap<String, ItemSimilarInfo>();
		for (Text val : values)
		{
			String[] item_list = val.toString().split("\t");
			for (int i = 0; i + 1 < item_list.length; i+=2)
			{
				ItemSimilarInfo info = new ItemSimilarInfo();
				info.item = item_list[i];
				info.weight = Double.parseDouble(item_list[i+1]);
				if (sim_map.containsKey(info.item))
				{
					ItemSimilarInfo old = sim_map.get(info.item);
					old.weight += info.weight;
					sim_map.put(info.item, old);
				}
				else
				{
					sim_map.put(info.item, info);
				}
			}
			
		}
		
		List<ItemSimilarInfo> sim_list = new ArrayList<ItemSimilarInfo>();
		for (String k : sim_map.keySet())
		{
			ItemSimilarInfo info = sim_map.get(k);
			sim_list.add(info);
		}
		Collections.sort(sim_list); // descend sort by similarity
		
		String lists = "";
		for (int i = 0; i < sim_list.size(); i++) // not record the count of each items
		{
			lists += (sim_list.get(i).toString() + "\t");
		}
		
		if (!lists.isEmpty())
		{
			String out = lists.substring(0, lists.length()-1);
			context.write(key, new Text(out));
		}
	}	
}