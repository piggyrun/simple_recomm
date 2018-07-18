package com.java.hadoop.pv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.java.hadoop.tools.LogStruct;

public class ParseLogMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		LogStruct logStruct = new LogStruct();
		String sig = logStruct.parse_raw_log(map_string);
		if(sig == null || logStruct.wuid == null)
			return;
		String vr_info = logStruct.getUserInfoString();
		if (vr_info != null)
		{
			context.write(new Text(logStruct.wuid), new Text(vr_info));
		}
	}
}