package com.cloudera.bigdata.analysis.solution.y;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.util.Util;

public class CubeGenerator {
	private final static Logger LOG = LoggerFactory
			.getLogger(CubeGenerator.class);
	final static String RECORD_THESHOLD_KEY = "qos.record.threshold";
	final static String FAIL_RATIO_THRESHOLD_KEY = "qos.fail.ratio.threshold";
	final static String TOP_FACTOR_NUMBER = "qos.top.count";
	final static String LINES_PER_MAPPER_KEY = "qos.lines.per.mapper";
	static long fails = 0;
	static long total = 0;
	static Random r = new Random();
	public CubeGenerator() {
		
	}
	
	public void triggerCompute(){
		
	}
	
	public static void main(String args[]) throws Exception{
		try {
			int linesPerMapper = 100000;
			if (args.length < 5) {
				System.err
						.println("Usage: CubeGenerator <input> <output> <LINES_PER_MAPPER> <recordThreshold> <failRatioThreshold>");
				System.exit(1);
			}

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(args[1]);
			fs.delete(outputPath, true);
			FSDataInputStream hdfsInStream = fs.open(new Path(args[5]));
			while(hdfsInStream.available() >0){
				String[] str = hdfsInStream.readLine().split(",");
				if(Long.parseLong(str[3]) > 0){
					fails++;
				}
				total++;	
			}
			conf.setLong("totalrecords", total);
			conf.setLong("totalfailRecords", fails);
			conf.set("simulate13",args[6]);
			hdfsInStream.close();
			linesPerMapper = Integer.parseInt(args[2]);

			//Util.setNumOfReduceTasks(conf, 1);
			Util.setProperty(conf, RECORD_THESHOLD_KEY, args[3]);
			Util.setProperty(conf, FAIL_RATIO_THRESHOLD_KEY, args[4]);
			//Util.setProperty(conf, TOP_FACTOR_NUMBER, args[5]);
			Job job = new Job(conf, "CubeGenerator");
			job.setJarByClass(CubeGenerator.class);
			job.setMapperClass(CubeMapper.class);
			job.setReducerClass(CubeReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(YMetrics.class);
			job.setInputFormatClass(NLineInputFormat.class);
			NLineInputFormat.setNumLinesPerSplit(job, linesPerMapper);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, outputPath);

			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error("", e);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error("", e);
		}

	}
}
