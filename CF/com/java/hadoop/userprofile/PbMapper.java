package com.java.hadoop.userprofile;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PbMapper extends Mapper<Object, Text, Text, Text> 
{
	public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
	{ 
		String map_string = map_value.toString();
		String[] infos = map_string.split("\t");
		if (infos.length < 5)
			return;
		String query = infos[0];
		String vrid = infos[2];
		String key = infos[4];
		
		if (vrid.startsWith("700007") || vrid.startsWith("700072")
		 || vrid.startsWith("700071") || vrid.startsWith("700100")) // movie VR
		{
			context.write(new Text("mv:" + query + "_0"), new Text(key));
		}
		else if (vrid.startsWith("700005")) // TV VR
		{
			context.write(new Text("tv:" + query + "_0"), new Text(key));
		}
		else if (vrid.startsWith("700010") || vrid.startsWith("700047")) // comic VR
		{
			context.write(new Text("cm:" + query + "_0"), new Text(key));
		}
		else if (vrid.startsWith("700008")) // show VR
		{
			context.write(new Text("sw:" + query + "_0"), new Text(key));
		}
	}
}