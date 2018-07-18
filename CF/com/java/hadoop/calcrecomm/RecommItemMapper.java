package com.java.hadoop.calcrecomm;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.java.hadoop.calcsimilarity.ItemSimilarInfo;


public class RecommItemMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] infos = map_string.split("\t");
		if (infos.length == 6) // item1 item2 occ uv1 occ uv2
		{
			ItemSimilarInfo item = new ItemSimilarInfo();
			item.getByFields(infos[1], infos[2], infos[3], infos[5]);
			context.write(new Text(infos[0]), new Text("i:" + item.toString()));
		}
	}
}