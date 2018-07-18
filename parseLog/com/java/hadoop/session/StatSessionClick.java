package com.java.hadoop.session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatSessionClick implements Tool 
{
	  private Configuration conf;
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 4) 
		  {
			  System.err.println("Usage: MS @ pv must contain <dir_in> <dir_in2> <out> <reduce num>");
			  System.exit(2);
		  }

		  Job job = configureJob(args);
		  return (job.waitForCompletion(true) ? 0 : 1);
	  }
	  
	  private Job configureJob(String[] args) throws IOException 
	  {
		  conf.set("mapred.output.compress", "true");
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
			
		  Job job = new Job(conf, "proess session");
		  
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileInputFormat.addInputPath(job, new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  int reduce_num = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(StatSessionClick.class);		    
		  job.setMapperClass(ClickMapper.class);
		  job.setReducerClass(ClickReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduce_num);
		  return job;
	  }

	  @Override
	  public Configuration getConf() {
		  return conf;
	  }

	  @Override
	  public void setConf(Configuration conf) {
		  this.conf = conf;
	  }

	  /**
	   * Main
	   * @param args
	   * @throws Exception
	   */
	  public static void main(String[] args) throws Exception
	  {
		  Configuration conf = new Configuration();
		  ToolRunner.run(conf, new StatSessionClick(), args);
	  }
}
