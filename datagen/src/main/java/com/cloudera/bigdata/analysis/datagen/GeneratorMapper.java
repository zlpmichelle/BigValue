package com.cloudera.bigdata.analysis.datagen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.datagen.GeneratorDriver.ParameterSet;
import com.cloudera.bigdata.analysis.dataload.Constants;

public class GeneratorMapper<KEYOUT, VALUEOUT> extends Mapper<LongWritable, Text, KEYOUT, VALUEOUT> {
  private final static Logger LOG = LoggerFactory.getLogger(GeneratorMapper.class);
  
  private ParameterSet parameterSet;
  public GeneratorMapper(){
    
  }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Path[] files = DistributedCache.getLocalCacheFiles(conf);
    if(files!=null){
      for (Path path : files) {
        if (path.getName().endsWith(conf.get(Constants.INSTANCE_DOC_NAME_KEY))) {
          conf.set(Constants.INSTANCE_DOC_PATH_KEY, path.toString());
          break;
        }
      }
    }else{
      LOG.error("No file in distributed cache");
    }
    
    parameterSet = new ParameterSet();
    parameterSet.instanceDoc = conf.get(Constants.INSTANCE_DOC_PATH_KEY);
    parameterSet.outputDir = conf.get("generator.outputDir");
    parameterSet.codec = conf.get("generator.codec");
    parameterSet.replicaNum = (short)conf.getInt("generator.replicaNum", GeneratorDriver.DEFAULT_REPLICA_NUM);
    parameterSet.useSizeMeasurement = conf.getBoolean("generator.useSizeMeasurement", true);
  }
  
  protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    LOG.info("Length" + value.getLength());
    String line = value.toString().trim();
    LOG.info("Groups: " + line);
    String[] parts = line.split(" ");
    List<Long> group = new ArrayList<Long>();
    for(String part:parts){
      group.add(Long.parseLong(part));
    }
    
    DateTime start = new DateTime();
    GeneratorTask task = new GeneratorTask(context.getConfiguration(), parameterSet, group, context.getTaskAttemptID().toString());
    task.run();
    
    while(task.isRunning()){
      Thread.sleep(5000);
      if(parameterSet.useSizeMeasurement){
        context.getCounter("GeneratorCounters", "GeneratedBytes").increment(task.getLastDone());
      }else{
        context.getCounter("GeneratorCounters", "GeneratedRecordsNum").increment(task.getLastDone());
      }
    }
    
    DateTime end = new DateTime();
    long duration = (end.getMillis() - start.getMillis())/1000;
    duration = duration==0?1:duration;
    LOG.info("***** Duration is: " + duration + " s");
    if(parameterSet.useSizeMeasurement){
      LOG.info("***** Accumulated Size: " + task.getAccumulated() + " MB");
      LOG.info("***** Generation Speed is: " + task.getAccumulated()/duration + " MB/s *****");
    }else{
      LOG.info("***** Total Number of Records: " + task.getTotal());
      LOG.info("***** Accumulated Size: " + task.getAccumulated() + " MB");
      LOG.info("***** Generation Speed is: " + task.getTotal()/duration + " record/s, or estimated: "
          + task.getAccumulated()/duration + " MB/s *****");
    }
  }
  
}
