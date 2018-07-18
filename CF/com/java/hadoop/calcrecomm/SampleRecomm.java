package com.java.hadoop.calcrecomm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SampleRecomm implements Tool 
{
	  private Configuration conf;
	  
	  public static class SampleUserMapper extends Mapper<Object, Text, Text, Text> 
	  {
		  // selected users
		  public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
		  { 
			  String map_string = map_value.toString();
			  String[] infos = map_string.split("\t");
			  if (infos.length > 1)
			  {
				  context.write(new Text(infos[0]), new Text("s"));
			  }
		  }	
	  }
	  
	  public static class SampleUPMapper extends Mapper<Object, Text, Text, Text> 
	  {
		  // user profiles
		  public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
		  { 
			  String map_string = map_value.toString();
			  String[] infos = map_string.split("\t");
			  if (infos.length > 2) // uid pv item1 item2 ...
			  {
				  context.write(new Text(infos[0]), new Text("u:" + map_string));
			  }
		  }
	  }
	  
	  public static class SampleResultMapper extends Mapper<Object, Text, Text, Text> 
	  {
		  // user recommended results
		  public void map(Object map_key, Text map_value, Context context) throws IOException, InterruptedException 
		  { 
			  String map_string = map_value.toString();
			  String[] infos = map_string.split("\t");
			  if (infos.length > 2) // uid item1 score1 item2 score2 ...
			  {
				  context.write(new Text(infos[0]), new Text("i:" + map_string));
			  }
		  }
	  }
	  
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 5)
		  {
			  System.err.println("Usage: <selected_users> <user_profile> <recomm_res> <out> <reduce num>");
			  System.exit(2);
		  }
		
		  Job job = configureJob(args);
		  boolean flag = job.waitForCompletion(true);
		  if (flag == false || job.isSuccessful() == false)
		  {
			  return 1;
		  }
		  
		  return 0;
	  }
	  
	  private Job configureJob(String[] args) throws IOException 
	  { 
		  System.out.println("recomm: sample_results");
		  Job job = new Job(conf, "recomm_sample_results");
		  
		  int reduceCount = Integer.parseInt(args[4]);
		  
		  job.setJarByClass(SampleRecomm.class);    
		  job.setReducerClass(SampleUserReducer.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduceCount);
		  
		  MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SampleUserMapper.class);
		  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SampleUPMapper.class);
		  MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, SampleResultMapper.class);
		  FileOutputFormat.setOutputPath(job, new Path(args[3]));
		  
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
		  ToolRunner.run(conf, new SampleRecomm(), args);
	  }
}
