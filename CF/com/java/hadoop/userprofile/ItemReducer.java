package com.java.hadoop.userprofile;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemReducer extends Reducer<Text, Text, Text, Text> 
{
	private String currName = "";
	private String prevKey = "";
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		String key_str = key.toString();
		if (key_str.length() < 6)
			return;
		
		String type = key_str.substring(0, 3);
		int keylen = key_str.length() - 2;

		String curKey = key_str.substring(0, keylen);
		String is_name = key_str.substring(keylen+1);
		if (is_name.equals("0"))
		{
			for (Text val : values)
			{
				currName = val.toString();
				prevKey = curKey;
				break;
			}
		}
		else
		{
			String newkey = "";
			if (type.equals("nv:") || type.equals("ap:"))
			{
				newkey = curKey;
			}
			else if (curKey.equals(prevKey))
			{
				newkey = type + currName;
			}
			else
			{
				newkey = curKey;
			}
			for (Text val : values)
			{
				context.write(new Text(newkey), val);
			}
		}
	}	
}