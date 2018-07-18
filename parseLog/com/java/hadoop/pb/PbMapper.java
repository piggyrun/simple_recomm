package com.java.hadoop.pb;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.java.hadoop.tools.PbStruct;

public class PbMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		PbStruct pb = new PbStruct();
		if (pb.parse_raw_log(map_string))
		{
			context.write(new Text(pb.toString()), null);
		}
	}
}