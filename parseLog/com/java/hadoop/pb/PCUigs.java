package com.java.hadoop.pb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PCUigs implements Tool {
	private Configuration conf;

	public static String urlDecode(String s, String charset) {
		String line = "";
		try {
			line = URLDecoder.decode(s, charset);
		} catch (IllegalArgumentException ex) {
			line = "";
		} catch (UnsupportedEncodingException ex) {
			line = "";
		}
		return line;
	}

	public static String changeCharset(String str, String oldCharset, String newCharset)
			throws UnsupportedEncodingException {
		String ret = "";
		if (str != null) {
			// 用旧的字符编码解码字符串。解码可能会出现异常。
			byte[] bs = str.getBytes();//oldCharset);
			// 用新的字符编码生成字符串
			ret = new String(bs, newCharset);
		}
		return ret;
	}
	
	public static class PcUigsMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		// selected users
		public void map(Object map_key, Text map_value, Context context)
				throws IOException, InterruptedException {
			String map_string = new String(map_value.getBytes(), "gbk");
			String[] infos = map_string.split("\t");
			if (infos.length > 4 && infos[0].equals("pvlog")) {
				String query = infos[4]; //changeCharset(infos[4], "gbk", "gbk");
				context.write(new Text(query), new IntWritable(1));
			}
		}
	}

	public static class QuerycntReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int cnt = 0;
			for (IntWritable val : values) {
				cnt += val.get();
			}
			context.write(key, new IntWritable(cnt));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// check input parameters
		if (args.length < 2) {
			System.err.println("Usage: MS @ pv must contain <dir_in> <out>");
			System.exit(2);
		}

		Job job = configureJob(args);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	private Job configureJob(String[] args) throws IOException {
		Job job = new Job(conf, "pc pingback");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(PCUigs.class);
		job.setMapperClass(PcUigsMapper.class);
		job.setReducerClass(QuerycntReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
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
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ToolRunner.run(conf, new PCUigs(), args);
	}
}
