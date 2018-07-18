package com.java.hadoop.calcrecomm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalcRecomm implements Tool 
{
	  private Configuration conf;
	  
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 4)
		  {
			  System.err.println("Usage: <user_profile> <item_sim> <out> <reduce num>");
			  System.exit(2);
		  }
		  
		  conf.set("mapred.output.compress", "true");
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
		
		  // item --> user & items
		  Job item_job = configureItemJob(args);
		  boolean flag = item_job.waitForCompletion(true);
		  if (flag == false || item_job.isSuccessful() == false)
		  {
			  return 1;
		  }
		  
		  // user --> items
		  Job user_job = configureUserJob(args);
		  boolean flag2 = user_job.waitForCompletion(true);
		  if (flag2 == false || user_job.isSuccessful() == false)
		  {
			  return 1;
		  }
		  
		  return 0;
	  }
	  
	  private Job configureItemJob(String[] args) throws IOException 
	  {	
		  System.out.println("recomm: get_item_user_list");
		  Job job = new Job(conf, "recomm_get_item_user_list");
		  
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(CalcRecomm.class);    
		  job.setReducerClass(RecommItemReducer.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduceCount);
		  
		  MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RecommUserItemMapper.class);
		  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RecommItemMapper.class);
		  FileOutputFormat.setOutputPath(job, new Path(args[2] + ".temp"));
		  
		  return job;
	  }
	  
	  private Job configureUserJob(String[] args) throws IOException 
	  {			
		  System.out.println("recomm: get_user_items");
		  Job job = new Job(conf, "recomm_get_user_items");
		  
		  FileInputFormat.addInputPath(job, new Path(args[2] + ".temp"));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(CalcRecomm.class);		    
		  job.setMapperClass(RecommUserMapper.class);	    
		  job.setReducerClass(RecommUserReducer.class);
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
		  ToolRunner.run(conf, new CalcRecomm(), args);
	  }
}
