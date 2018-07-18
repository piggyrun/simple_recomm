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

public class OutputSimilarity implements Tool 
{
	  private Configuration conf;
	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 3)
		  {
			  System.err.println("Usage: <log_in> <out> <reduce num> [<is_debug>]");
			  System.exit(2);
		  }

		  Job job = configureJob(args);
		  return (job.waitForCompletion(true) ? 0 : 1);
	  }
	  
	  private Job configureJob(String[] args) throws IOException 
	  {
		  int reduceCount = Integer.parseInt(args[2]);
		  String is_debug = "false";
		  if (args.length >= 4)
		  {
			  is_debug = args[3];
		  }
		  conf.set("recomm.calcsimilarity.outdebug", is_debug);
		  conf.set("mapred.output.compress", "true");
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
			
		  System.out.println("recomm: output_similarity");
		  Job job = new Job(conf, "recomm_output_similarity");
		  
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		  job.setJarByClass(OutputSimilarity.class);		    
		  job.setMapperClass(OutputMapper.class);	    
		  job.setReducerClass(OutputReducer.class);
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
		  ToolRunner.run(conf, new OutputSimilarity(), args);
	  }
}
