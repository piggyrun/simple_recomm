package com.java.hadoop.calcsimilarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NormSimilarity implements Tool 
{
	  private Configuration conf;
	  private String outprefix;
	  
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length != 4)
		  {
			  System.err.println("Usage: <sim_in> <itemUV> <out> <reduce num>");
			  System.exit(2);
		  }
		  
		  outprefix = args[2];
		  conf.set("mapred.output.compress", "true");
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
		
		  // collect UV for each item in pairs
		  Job norm_job = configureNormJob(args);
		  boolean flag = norm_job.waitForCompletion(true);
		  if (flag == false || norm_job.isSuccessful() == false)
		  {
			  return 1;
		  }
		  
		  // get occ and UVs for normalized pair weight
		  Job calc_job = configureCalcJob(args);
		  boolean flag2 = calc_job.waitForCompletion(true);
		  if (flag2 == false || calc_job.isSuccessful() == false)
		  {
			  return 1;
		  }
		  
		  return 0;
	  }
	  
	  private Job configureNormJob(String[] args) throws IOException 
	  {	
		  System.out.println("recomm: get_norm_similarity");
		  Job job = new Job(conf, "recomm_get_norm_similarity");
		  
		  FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(outprefix + ".temp"));
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(NormSimilarity.class);		    
		  job.setMapperClass(GetNormMapper.class);	    
		  job.setReducerClass(GetNormReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduceCount);
		  
		  return job;
	  }
	  
	  private Job configureCalcJob(String[] args) throws IOException 
	  {			
		  System.out.println("recomm: get_norm_similarity");
		  Job job = new Job(conf, "recomm_get_norm_similarity");
		  
		  FileInputFormat.addInputPath(job, new Path(outprefix + ".temp"));
		  FileOutputFormat.setOutputPath(job, new Path(outprefix));
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(NormSimilarity.class);		    
		  job.setMapperClass(CalcNormMapper.class);	    
		  job.setReducerClass(CalcNormReducer.class);
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
		  ToolRunner.run(conf, new NormSimilarity(), args);
	  }
}
