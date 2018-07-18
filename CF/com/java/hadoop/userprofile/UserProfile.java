package com.java.hadoop.userprofile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UserProfile implements Tool 
{
	  private Configuration conf;
	  
	  public static class ItemPartitioner extends Partitioner<Text, Object>
	  {
		  public int getPartition(Text key, Object val, int numPartitions)
		  {
			  String str = key.toString();
			  String query = str.substring(0, str.length()-2);
			  HashPartitioner<Text, Object> p = new HashPartitioner<Text, Object>();
			  return p.getPartition(new Text(query), null, numPartitions);
		  }
	  }

	  @Override
	  public int run(String[] args) throws Exception
	  {
		  // check input parameters
		  if (args.length < 4)
		  {
			  System.err.println("Usage: <resinlog> <pblog> <out> <reduce num>");
			  System.exit(2);
		  }

		  conf.set("mapred.output.compress", "true");
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
		  
		  // switch item name from query to key
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
		  System.out.println("recomm: user_profile1");
		  Job job = new Job(conf, "recomm_user_profile1");
		  
		  // dir structure: args[0]/yyyymm/yyyymmdd/
		  FileSystem filesys = FileSystem.get(conf);
		  FileStatus fsDaemonMonthDirs[] = filesys.listStatus(new Path(args[0]));
		  FileStatus fsDaemonPbDirs[] = filesys.listStatus(new Path(args[1]));
		  for (int i = 0; i < fsDaemonMonthDirs.length; i ++)
		  {
			  if (fsDaemonMonthDirs[i].isDir())
			  {
				  FileStatus fsDaemonDateDirs[] = filesys.listStatus(fsDaemonMonthDirs[i].getPath());
				  for (int j = 0; j < fsDaemonDateDirs.length; j++)
				  {
					  MultipleInputs.addInputPath(job, fsDaemonDateDirs[j].getPath(), TextInputFormat.class, ResinMapper.class);
				  }
			  }
		  }
		  for (int i = 0; i < fsDaemonPbDirs.length; i ++)
		  {
			  MultipleInputs.addInputPath(job, fsDaemonPbDirs[i].getPath(), TextInputFormat.class, PbMapper.class);
		  }
		
		  //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ResinMapper.class);
		  //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PbMapper.class);
		  FileOutputFormat.setOutputPath(job, new Path(args[2] + ".temp"));
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(UserProfile.class);   
		  job.setPartitionerClass(ItemPartitioner.class);
		  job.setReducerClass(ItemReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setNumReduceTasks(reduceCount);
		  
		  return job;
	  }
	  
	  private Job configureUserJob(String[] args) throws IOException 
	  {			
		  System.out.println("recomm: user_profile2");
		  Job job = new Job(conf, "recomm_user_profile2");
		  
		  FileInputFormat.addInputPath(job, new Path(args[2] + ".temp"));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  int reduceCount = Integer.parseInt(args[3]);
		  
		  job.setJarByClass(UserProfile.class);		    
		  job.setMapperClass(UPMapper.class);	    
		  job.setReducerClass(UPReducer.class);
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
		  ToolRunner.run(conf, new UserProfile(), args);
	  }
}
