package com.java.hadoop.userprofile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopUser implements Tool 
{
	  private Configuration conf;
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 3)
		  {
			  System.err.println("Usage: <log_in> <out> <reduce num>");
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
			
		  System.out.println("recomm: popular_user");
		  Job job = new Job(conf, "recomm_popular_user");
		  
		  FileInputFormat.setInputPaths(job, new Path(args[0]));						
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  int reduceCount = Integer.parseInt(args[2]);
		  
		  job.setJarByClass(PopUser.class);		    
		  job.setMapperClass(PopUMapper.class);	    
		  job.setReducerClass(PopUReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduceCount);
		  
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
		  ToolRunner.run(conf, new PopUser(), args);
	  }
}
