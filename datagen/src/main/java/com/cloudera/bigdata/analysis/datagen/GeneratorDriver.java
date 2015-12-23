package com.cloudera.bigdata.analysis.datagen;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/**
 * You can specify following parameters while invoking the driver:
 * 1. <instanceDoc>,<runningMode>,<parallelism>,<total_size>,<min_size>,<max_size>
 * 2. <instanceDoc>,<runningMode>,<parallelism>,<total_num>,<min_num>,<max_mum>
 * 
 * As measured, the generation speed for each mapper is about 70MB/sec (without writing to disk).
 * The mapper number for each node can be set as ~1.5x of the disk number.
 * 
 * If compression is enabled, you should be aware that the compression speed may 
 * be slower than disk write speed.
 */
public class GeneratorDriver {
  private static final Logger LOG = LoggerFactory.getLogger(GeneratorDriver.class);

  private static final String INSTANCE_DOC_KEY = "instanceDoc";
  private static final String RUNNING_MODE_KEY = "mode";
  private static final String PARALLEL_NUM_KEY = "parallel";
  private static final String OUTPUT_FOLDER_KEY = "outputDir";
  private static final String REPLICATION_NUM_KEY = "replicaNum";
  private static final String CODEC_KEY = "codec";
  private static final String TOTAL_SIZE_KEY = "totalSize";
  private static final String MAX_SIZE_KEY = "maxSize";
  private static final String MIN_SIZE_KEY = "minSize";
  private static final String TOTAL_NUM_KEY = "totalNum";
  private static final String MAX_NUM_KEY = "maxNum";
  private static final String MIN_NUM_KEY = "minNum";
  private static final String DEBUG_KEY = "debug";
  private static final String NEVERSTOP_KEY = "neverStop";
  private static final String DEFAULT_RUNNING_MODE = "mapred";
  private static final String LOCAL_RUNNING_MODE = "local";
  private static final String GENERATOR_CONF_FOLDER_HDFS = "generator/tmp/";
  private static final String GENERATOR_CONF_FILE = "groups";
  public static final short DEFAULT_REPLICA_NUM = 1;
  private static final short MAX_REPLICA_NUM = 3;
  private static final int DEFAULT_PARALLELISM = 1;
  private static final long DEFAULT_TOTAL_SIZE = 1L;  // GB
  private static final long DEFAULT_MINIMUM_SIZE = 10L;  // MB
  private static final long DEFAULT_MAXIMUM_SIZE = 100L; // MB
  
  private static final long DEFAULT_TOTAL_NUM = new Double(1e8).longValue();
  private static final long DEFAULT_MINIMUM_NUM = new Double(1e6).longValue();
  private static final long DEFAULT_MAXIMUM_NUM = new Double(1e7).longValue();
  
  private ExecutorService executorService;
  
  private ParameterSet parameterSet;
  private List<List<Long>> groups;
  
  public GeneratorDriver(ParameterSet paramSet){
    this.parameterSet = paramSet;
    
    // calculate the file sequence
    FastCalculator calculator = new FastCalculator(parameterSet);
    calculator.awaitFinish();
    groups = calculator.group();
    
    if(parameterSet.runningMode.equals(DEFAULT_RUNNING_MODE)){
      Configuration conf = new Configuration();
      Path path = new Path(parameterSet.outputDir);
      try {
        FileSystem fileSystem = path.getFileSystem(conf);
        if(!(fileSystem instanceof DistributedFileSystem)){
          LOG.error("Must set the *HDFS* folder in '" + OUTPUT_FOLDER_KEY + "' option for mapred generation.");
          System.exit(1);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }else{
      executorService = Executors.newFixedThreadPool(parameterSet.parallelism);
      
    }
  }
  
  public void start(){
    DateTime start = new DateTime();
    
    if(parameterSet.runningMode.equals(DEFAULT_RUNNING_MODE)){
      runAsMapRedMode();
    }else{
      runSingleClient();
    }
    
    DateTime end = new DateTime();
    long duration = (long)((end.getMillis() - start.getMillis())/1000);
    if(parameterSet.useSizeMeasurement){
      long total = parameterSet.totalSize * 1024;
      
      System.out.println("The generation speed is: " + total/duration + " MB/s");
    }else{
      System.out.println("The generation speed is: " + parameterSet.totalNum/duration + " record/s");
    }
  }
  
  public void stop(){
    //TODO: add stop implementation
  }
  
  public void runSingleClient(){
    for(int i=0;i<groups.size();i++){
      GeneratorTask task = new GeneratorTask(
          new Configuration(), parameterSet, groups.get(i), new Integer(i).toString());
      executorService.submit(task);
    }
    
    executorService.shutdown();
    
    while(!executorService.isTerminated()){
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    LOG.info("Successfully generated");
  }
  
  public void runAsMapRedMode(){
    try{
      Job job = new Job(new Configuration());
      Configuration conf = job.getConfiguration();
      FileSystem hdfs = FileSystem.get(conf);
      Path homeDir = hdfs.getHomeDirectory();
      LOG.info("HomeDir: " + homeDir.makeQualified(hdfs));
      hdfs.delete(new Path(homeDir, GENERATOR_CONF_FOLDER_HDFS), true);
      Path inputPath = new Path(homeDir, GENERATOR_CONF_FOLDER_HDFS + UUID.randomUUID().toString());
      FSDataOutputStream outputStream = hdfs.create(new Path(inputPath + "/" + GENERATOR_CONF_FILE));
      PrintWriter printWriter = new PrintWriter(outputStream);
      for(int i=0;i<groups.size();i++){
        List<Long> group = groups.get(i);
        StringBuilder builder = new StringBuilder();
        for(Long value:group){
          builder.append(value);
          builder.append(" ");
        }
        builder.append("\n");
        String value = builder.toString().trim();
        if(value.length()>0){
          LOG.debug("Length: " + value.length());
          printWriter.println(value);
        }
      }
      printWriter.close();
      
      String fileName = parameterSet.instanceDoc.substring(parameterSet.instanceDoc
          .lastIndexOf('/') + 1);
      conf.set(Constants.INSTANCE_DOC_NAME_KEY, fileName);
      String qualifiedPath = Util.makeQualified(parameterSet.instanceDoc, conf);
      LOG.info(qualifiedPath);
      conf.set("tmpfiles", qualifiedPath);
      conf.set("generator.outputDir", parameterSet.outputDir);
      if(null!=parameterSet.codec){
        conf.set("generator.codec", parameterSet.codec);
      }
      conf.setInt("generator.replicaNum", parameterSet.replicaNum);
      conf.setBoolean("generator.useSizeMeasurement", parameterSet.useSizeMeasurement);
      conf.set(Constants.TASK_TIMEOUT_KEY, "0");
      conf.set(Constants.MAPPER_SPECULATIVE_KEY, "false");
      conf.set(Constants.REDUCER_SPECULATIVE_KEY, "false");
      NLineInputFormat.setInputPaths(job, inputPath);
      NLineInputFormat.setNumLinesPerSplit(job, 1);
      job.setInputFormatClass(NLineInputFormat.class);
      job.setJarByClass(GeneratorMapper.class);
      job.setMapperClass(GeneratorMapper.class);
      job.setJobName("Generator-" + UUID.randomUUID().toString());
      job.setNumReduceTasks(0);
      FileOutputFormat.setOutputPath(job, new Path(inputPath.toString() + "_O"));
      
      job.waitForCompletion(true);
      
      // try to clean the generator configuration folder
      hdfs.delete(new Path(homeDir, GENERATOR_CONF_FOLDER_HDFS), true);
    }catch (Exception e) {
      LOG.error("", e);
    }
  }
  
  private static ParameterSet parseArguments(String[] args){
    OptionParser optionParser = new OptionParser();
    
    OptionSpec<String> instanceDocOption = optionParser.accepts(INSTANCE_DOC_KEY, 
        "xml configuration file for generator, refer generator.xsd for syntax")
        .withRequiredArg().ofType(String.class).required();
    OptionSpec<String> runningModeOption = optionParser.accepts(RUNNING_MODE_KEY, 
        "the running mode for generator, other than 'mapred' will be in single client mode")
        .withOptionalArg().ofType(String.class).defaultsTo(DEFAULT_RUNNING_MODE);
    OptionSpec<Integer> parallelismOption = optionParser.accepts(PARALLEL_NUM_KEY, 
        "the parallel number for generator, expected mapper number in mapred mode, threads number in single client mode")
        .withOptionalArg().ofType(Integer.class).defaultsTo(DEFAULT_PARALLELISM);
    OptionSpec<String> outputDirOption = optionParser.accepts(OUTPUT_FOLDER_KEY, 
        "the *absolution* output directory for generator; " +
        "for windows local, it should be specified as 'file:///f:/xxx'; " +
        "for linux local, it should be specified as 'file:///tmp/xxx'; " +
        "for HDFS, should be specified as hdfs://<namenode>:8020/xxx, make sure the folder is writable.")
        .withRequiredArg().ofType(String.class).required();
    OptionSpec<Short> replicaNumOption = optionParser.accepts(REPLICATION_NUM_KEY, 
        "the default replication number for data generated in HDFS")
        .withOptionalArg().ofType(Short.class).defaultsTo(DEFAULT_REPLICA_NUM);
    OptionSpec<String> codecOption = optionParser.accepts(CODEC_KEY,
        "compression codec for generated source")
        .withOptionalArg().ofType(String.class);
    OptionSpec<Boolean> neverStopOption = optionParser.accepts(NEVERSTOP_KEY, 
        "if the generation never stops")
        .withOptionalArg().ofType(Boolean.class);
    
    OptionSpec<Long> totalSizeOption = addOption(
        optionParser, TOTAL_SIZE_KEY, "total size (GB) need to be generated", DEFAULT_TOTAL_SIZE);
    OptionSpec<Long> minSizeOption = addOption(
        optionParser, MIN_SIZE_KEY, "minimum size (MB) generated for a single file", DEFAULT_MINIMUM_SIZE);
    OptionSpec<Long> maxSizeOption = addOption(
        optionParser, MAX_SIZE_KEY, "maximum size (MB) generated for a single file", DEFAULT_MAXIMUM_SIZE);
    
    OptionSpec<Long> totalNumOption = addOption(
        optionParser, TOTAL_NUM_KEY, "total number of records need to be generated", DEFAULT_TOTAL_NUM);
    OptionSpec<Long> minNumOption = addOption(
        optionParser, MIN_NUM_KEY, "minimum number of records generated for a single file", DEFAULT_MINIMUM_NUM);
    OptionSpec<Long> maxNumOption = addOption(
        optionParser, MAX_NUM_KEY, "maximum number of records generated for a single file", DEFAULT_MAXIMUM_NUM);
    
    OptionSpec<Boolean> debugOption = optionParser.accepts(DEBUG_KEY, "if to enable debug")
        .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
    
    OptionSet optionSet = null;
    try{
      optionSet = optionParser.parse(args);
    }catch(OptionException e){
      try {
        optionParser.printHelpOn(System.out);
        e.printStackTrace();
      } catch (IOException e1) {
        LOG.error("Exception occurred while print help message: ", e);
        System.exit(1);
      }
    }
    
    String instanceDoc = optionSet.valueOf(instanceDocOption);
    String outputDir = optionSet.valueOf(outputDirOption);
    short replicaNum = optionSet.valueOf(replicaNumOption);
    if(replicaNum<1 || replicaNum>MAX_REPLICA_NUM){
      LOG.warn("Not expected replica number: " + replicaNum 
          + ", set it to defaut num: " + DEFAULT_REPLICA_NUM);
    }
    String runningMode = DEFAULT_RUNNING_MODE;
    int parallelism = DEFAULT_PARALLELISM;
    if(optionSet.has(runningModeOption)){
      runningMode = optionSet.valueOf(runningModeOption);
    }
    if(optionSet.has(parallelismOption)){
      parallelism = optionSet.valueOf(parallelismOption);
    }
    
    boolean debug = false;
    if(optionSet.has(debugOption)){
      debug = optionSet.valueOf(debugOption);
    }
    
    String codec = null;
    if(optionSet.has(codecOption)){
      codec = optionSet.valueOf(codecOption);
    }
    
    boolean neverStop = false;
    if(optionSet.has(neverStopOption)){
      neverStop = optionSet.valueOf(neverStopOption);
    }
    
    boolean useSize = true;
    long totalSize = DEFAULT_TOTAL_SIZE;
    long totalNum = DEFAULT_TOTAL_NUM;
    if(optionSet.has(totalSizeOption)||!optionSet.has(totalNumOption)){
      useSize = true;
      if(optionSet.has(totalSizeOption)){
        totalSize = optionSet.valueOf(totalSizeOption);
      }
    }else{
      useSize = false;
      totalNum = optionSet.valueOf(totalNumOption);
    }
    
    long minSize = DEFAULT_MINIMUM_SIZE;
    long maxSize = DEFAULT_MAXIMUM_SIZE;
    long minNum = DEFAULT_MINIMUM_NUM;
    long maxNum = DEFAULT_MAXIMUM_SIZE;
    if(useSize){
      if(optionSet.has(minSizeOption)){
        minSize = optionSet.valueOf(minSizeOption);
      }
      if(optionSet.has(maxSizeOption)){
        maxSize = optionSet.valueOf(maxSizeOption);
      }
      
      preCheck(useSize, totalSize, maxSize, minSize);
    }else{
      if(optionSet.has(minNumOption)){
        minNum = optionSet.valueOf(minNumOption);
      }
      if(optionSet.has(maxNumOption)){
        maxNum = optionSet.valueOf(maxNumOption);
      }
      
      preCheck(useSize, totalNum, maxNum, minNum);
    }
    
    ParameterSet parameterSet = new ParameterSet();
    parameterSet.instanceDoc = instanceDoc;
    parameterSet.outputDir = outputDir;
    parameterSet.replicaNum = replicaNum;
    parameterSet.runningMode = runningMode;
    parameterSet.parallelism = parallelism;
    parameterSet.codec = codec;
    parameterSet.useSizeMeasurement = useSize;
    parameterSet.totalSize = totalSize;
    parameterSet.minSize = minSize;
    parameterSet.maxSize = maxSize;
    parameterSet.totalNum = totalNum;
    parameterSet.minNum = minNum;
    parameterSet.maxNum = maxNum;
    parameterSet.debug = debug;
    parameterSet.neverStop = neverStop;
    
    preCheck(parameterSet);
    
    return parameterSet;
  }
  
  private static OptionSpec<Long> addOption(
      OptionParser parser, 
      String option, 
      String description, 
      long defaultValue){
    return parser.accepts(option, description).withOptionalArg().ofType(Long.class).defaultsTo(defaultValue);
  }
  
  private static void preCheck(ParameterSet parameterSet){
    if(!(DEFAULT_RUNNING_MODE.equalsIgnoreCase(parameterSet.runningMode)
        ||LOCAL_RUNNING_MODE.equalsIgnoreCase(parameterSet.runningMode))){
      LOG.warn("Unrecognized running mode: " + parameterSet.runningMode 
          + ", correct value should be '" + DEFAULT_RUNNING_MODE 
          + "' or '" + LOCAL_RUNNING_MODE + "'");
      System.exit(1);
    }
  }
  
  private static void preCheck(boolean useSize, long total, long max, long min){
    if(max<min){
      LOG.warn("The --" + (useSize?"maxSize":"maxNum") + " is smaller than --" + (useSize?"minSize":"minNum") + ". Quitting......");
      System.exit(1);
    }
    
    if((total<<10) <max){
      LOG.warn("The --" + (useSize?"totalSize":"totalNum") + " is smaller than --" + (useSize?"maxSize":"maxNum") + ". Quitting......");
      System.exit(1);
    }
  }
  
  public static class ParameterSet {
    String instanceDoc;
    String outputDir;
    short replicaNum;
    String runningMode;
    int parallelism;
    String codec;
    boolean useSizeMeasurement;
    long totalSize;
    long minSize;
    long maxSize;
    long totalNum;
    long minNum;
    long maxNum;
    boolean debug;
    boolean neverStop;
    
    public String toString(){
      return ("instanceDoc: " + instanceDoc
          + "\noutputDir: " + outputDir
          + "\nreplicaNum: " + replicaNum
          + "\nrunningMode: " + runningMode
          + "\nparallelism: " + parallelism
          + "\ncodec: " + codec
          + "\nuseSizeMeasurement: " + useSizeMeasurement
          + "\ntotalSize: " + totalSize
          + "\nminSize: " + minSize
          + "\nmaxSize: " + maxSize
          + "\ntotalNum: " + totalNum
          + "\nminNum: " + minNum
          + "\nmaxNum: " + maxNum
          + "\ndebug: " + debug
          + "\nneverStop: " + neverStop);
    }
  }
  
  private static class ShutdownHandler extends Thread {
    private GeneratorDriver generatorDriver;
    public ShutdownHandler(GeneratorDriver driver){
      this.generatorDriver = driver;
    }
    
    public void run(){
      
    }
  }
  
  public static void main(String[] args){
    ParameterSet parameterSet = parseArguments(args);
    GeneratorDriver driver = new GeneratorDriver(parameterSet);
    driver.start();
    Runtime.getRuntime().addShutdownHook(new ShutdownHandler(driver));
  }
}
