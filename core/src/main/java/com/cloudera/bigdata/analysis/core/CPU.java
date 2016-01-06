package com.cloudera.bigdata.analysis.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Michelle on 14-1-28.
 */
public class CPU {

  public static class CPUThread extends Thread {

    @Override
    public void run() {
      long now = System.currentTimeMillis();
      while (System.currentTimeMillis() - now < 10000) {
      }
      System.out.println("start time: " + now);
      System.out.println("end time: " + System.currentTimeMillis());
      System.out.println("-------------------------" + this.getName());
    }

  }

  public static class CPUMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // waste cpu
      ExecutorService executor = Executors.newFixedThreadPool(1);
      for (int i = 0; i < 1; i++) {
        Runnable worker = new CPUThread();
        executor.execute(worker);
      }
      executor.shutdown();
      while (!executor.isTerminated()) {
      }
      System.out.println("Finished all threads in 5min");
    }
  }

public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
        if (otherArgs.length != 2) {
        System.err.println("Usage: cpu <in> <out>");
        System.exit(2);
        }

        Job job = new Job(conf, "cpu");
    job.setJarByClass(CPU.class);
    job.setMapperClass(CPUMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
