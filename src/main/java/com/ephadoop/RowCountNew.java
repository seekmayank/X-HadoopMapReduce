package com.ephadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoopMapper.MapperRecord;
import com.hadoopReducer.ReducerRecordCount;


public class RowCountNew extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RowCount(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(),
				"Row Count using built in mappers and reducers");

		job.setJarByClass(getClass());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// We are not setting input format class and hence uses
		// (TextInputFormat)
		// job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(MapperRecord.class);
		// Custom mapper (RecordMapper) to assign 1 for each record
		// Input to mapper <Lineoffset as key, entire line as value>
		// Default behavior of default input format TextInputFormat

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Output from mapper
		// <count, 1>
		// <count, 1> so on

		job.setReducerClass(ReducerRecordCount.class);
		// Built-in reducer
		// Input to reducer <count, {1, 1, 1, ...}>
		// job.setReducerClass(IntSumReducer.class);
		// Output from reducer <count, Number of records>

		// Custom reducer
		// If you do not want to see "count" as part of output
		// and just see the record count as in select count query

		// Output from reducer <Number of records>

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// We are not setting output format class and hence uses default
		// (TextOutputFormat)
		
		// job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}