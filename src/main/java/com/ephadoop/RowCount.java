package com.ephadoop;

import java.io.IOException;

//import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RowCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new RowCount(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(),"Map Reduce File Count");
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setMapperClass(RecordMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Output from reducer <Number of records>
		job.setReducerClass(NoKeyRecordCountReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// We are not setting output format class and hence uses default
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class RecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		//public RecordMapper(){}

		@Override
		public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException
		{
			context.write(new Text("count"), new IntWritable(1));
		}
	}
	
	public static class NoKeyRecordCountReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> records, Context context) throws IOException, InterruptedException
		{
			int sum = 0;

			for (IntWritable record : records) {
				sum += record.get();
			}
			context.write(NullWritable.get(), new IntWritable(sum));
		}
	}

}
