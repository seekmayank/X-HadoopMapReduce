package com.ephadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.ebay.hmc.hadoop.Util.CompressionCodeName;
//import com.hadoop.compression.lzo.LzopCodec;

public class MergeTextFile {

	private static final Logger logger = LoggerFactory.getLogger(MergeTextFile.class);

	public static Job submitMergeTextFiles(Configuration conf, String queue, String inputFolder, 
			List<List<String>> inputFiles, String outputFolder, List<String> outputFileNames, String compressionCode) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		Job job = Util.generateJob(conf, queue, inputFolder, inputFiles, outputFolder, outputFileNames, compressionCode);
		job.setMapperClass(MergeMapper.class);

		job.submit();
		return job;
	}
	

	
	
	public static class MergeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			String[] temp = value.toString().split(";");
			String[] inputFiles = temp[0].split(",");
			Path outputFile = new Path(context.getConfiguration().get(Util.OUTPUTFOLDER), temp[1]);
			
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(outputFile)){
				fs.delete(outputFile, false);
			}
			
			
			List<Path> inputPaths = Arrays.asList(inputFiles).stream().map(f->new Path(context.getConfiguration().get(Util.INPUTFOLDER), f)).collect(Collectors.toList());
			
			String outputCompressionCode = conf.get(Util.COMPRESSIONCODE);
			
			long totalReadLines = 0l;
			long firstFileLine = 0l;
			long totalWriteLines = 0l;
			
			try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getOutStream(conf, outputCompressionCode, outputFile)))){
							
				for(Path path:  inputPaths){
					
					long lineCount = 0;
					try(BufferedReader fin = new BufferedReader(new InputStreamReader(getInputStream(conf, path)))){
						String line = fin.readLine();
						 while(null != line){
							 totalReadLines++;
							 writer.write(line);
							 writer.newLine();
							 totalWriteLines++;
							 line = fin.readLine();
							 lineCount ++ ;
						 }
					}
					
					if(firstFileLine == 0){
						firstFileLine = lineCount;
					}
					writer.flush();
				}
			}
			
			context.write(new Text(String.format("%s,%s,%s,%s", temp[1], firstFileLine, totalReadLines,totalWriteLines)), NullWritable.get());
		}

	}
	
	private static InputStream getInputStream(Configuration conf, Path path) throws IOException {

		FSDataInputStream i = FileSystem.get(conf).open(path);
		// Handle 0 and 1-byte files
	      short leadBytes;
	      try {
	        leadBytes = i.readShort();
	      } catch (EOFException e) {
	        i.seek(0);
	        return i;
	      }

	      // Check type of stream first
	      switch(leadBytes) {
	        case 0x1f8b: { // RFC 1952
	          // Must be gzip
	          i.seek(0);
	          return new GZIPInputStream(i);
	        }
	        default: {
	          // Check the type of compression instead, depending on Codec class's
	          // own detection methods, based on the provided path.
	          CompressionCodecFactory cf = new CompressionCodecFactory(conf);
	          CompressionCodec codec = cf.getCodec(path);
	          if (codec != null) {
	            i.seek(0);
	            return codec.createInputStream(i);
	          }
	          break;
	        }
	      }

	      // File is non-compressed, or not a file container we know.
	      i.seek(0);
	      return i;
	  
	}
	
	private static OutputStream getOutStream(Configuration conf, String compType, Path outpath)
			throws IOException {

		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream  fout = fs.create(outpath);

		if (compType == null)
			return fout;
		/*
		else if (compType.equals(CompressionCodeName.GZIP.toString())) {
			GzipCodec codec = new GzipCodec();
			codec.setConf(conf);
			return codec.createOutputStream(fout);
		} else if (compType.equals(CompressionCodeName.BZIP.toString()))
			return new BZip2Codec().createOutputStream(fout);
		else if (compType.equals(CompressionCodeName.LZO.toString())) {
			LzopCodec codec = new LzopCodec();
			codec.setConf(conf);
			return codec.createOutputStream(fout);
		} */
		else
			throw new RuntimeException("CompType not recognized");

		
	}
	

}