package com.ephadoop;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.ebay.hmc.hadoop.MergeTextFile.MergeMapper;

public class Util {
	
	private static final Logger logger = LoggerFactory.getLogger(Util.class);

	public static final String PREFIX = "_BDPMERGE";
	public static final String METAFOLDER="bdp.metafolder";
	public static final String OUTPUTFOLDER="bdp.outputFolder";
	public static final String INPUTFOLDER="bdp.inputFolder";
	public static final String COMPRESSIONCODE="bdp.compressionCode";
	public static final String META_FOLDER = "_bdp_meta";
	public static final String META_OUTPUTFOLDER = "output";
	
	private static int RETRY_TIMES = 3;
	
	public static enum CompressionCodeName {
		GZIP, BZIP, LZO, SNAPPY
	}
	
	public static boolean generateInputFile(List<List<String>> inputFiles, List<String> outputFiles, Path inputPath, FileSystem fs){
		
		for(int i = 0 ; i < RETRY_TIMES; i++){
			try{			
				FSDataOutputStream streamWriter = fs.create(inputPath);
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(streamWriter));
				for(int j = 0; j < inputFiles.size() ; j++){	
					writer.write(String.join(",", inputFiles.get(j)) + ";" + outputFiles.get(j));
					writer.newLine();
					writer.flush();
					
				}
				writer.close();
				
				if(fs.exists(inputPath)){
					return true;
				}
			}catch(Exception e){
				logger.error("Fail to generate input file.", e);
			}
			
		}
		
		return false;
		
	}
	
//	public static class SplittableTextInputFormat extends TextInputFormat{
//
//		@Override
//		protected boolean isSplitable(JobContext context, Path file) {
//			// TODO Auto-generated method stub
//			return true;
//		}
//		
//	}
	
	public static Job generateJob(Configuration conf, String queue, String inputFolder, 
			List<List<String>> inputFiles, String outputFolder, List<String> outputFileNames, String compressionCode) throws IOException{
		
		FileSystem fs = FileSystem.newInstance(conf);
		
		try{
			Path metaFolderPath = new Path(outputFolder, Util.META_FOLDER);
			if(fs.exists(metaFolderPath)){
				fs.delete(metaFolderPath, true);
			}
			fs.mkdirs(metaFolderPath);
			
			Path inputFile = new Path(metaFolderPath, "fileList");
			if(!Util.generateInputFile(inputFiles, outputFileNames, inputFile, fs)){
				logger.error("Fail to generate input file list.");
				return null;
			}
			
			Job job = Job.getInstance(conf, "BDP Merge files");
			job.setJarByClass(MergeTextFile.class);

			job.getConfiguration().set("mapreduce.job.queuename", queue);
			job.getConfiguration().set(Util.METAFOLDER, metaFolderPath.toUri().toString());
			job.getConfiguration().set(Util.OUTPUTFOLDER, outputFolder);
			job.getConfiguration().set(Util.INPUTFOLDER, inputFolder);
			//max map timeout is 30 min
			job.getConfiguration().set("mapreduce.task.timeout", new Long(30*60*1000).toString());
			
			job.setNumReduceTasks(0);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setInputFormatClass(NLineInputFormat.class);
			job.getConfiguration().set("mapreduce.input.lineinputformat.linespermap", "1");
			
			job.getConfiguration().set("mapreduce.job.maps", new Integer(outputFileNames.size()).toString());
			
			if(null != compressionCode)
				job.getConfiguration().set(Util.COMPRESSIONCODE, compressionCode);
			
			job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
			job.getConfiguration().set("mapreduce.map.output.compress", "false");
			job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "false");
			job.setReduceSpeculativeExecution(false);
			job.setReducerClass(GeneralMergeReduce.class);
			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, new Path(metaFolderPath, META_OUTPUTFOLDER));
			
			return job;
		}finally{
			if(null != fs){
				try{
					fs.close();
				}catch(Exception e){
					logger.error("Fail to close file system.", e);
				}
				
			}
		}
		
	}
	
	public static class GeneralMergeReduce extends Reducer<Text, NullWritable, Text, NullWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<NullWritable> arg1,
				Reducer<Text, NullWritable, Text, NullWritable>.Context arg2)
				throws IOException, InterruptedException {
			arg2.write(arg0, NullWritable.get());
		}
		
	}
}