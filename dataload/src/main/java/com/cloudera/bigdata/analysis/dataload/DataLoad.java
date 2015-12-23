package com.cloudera.bigdata.analysis.dataload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.io.DataFileInputFormat;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.io.FileObjectArrayWritable;
import com.cloudera.bigdata.analysis.dataload.mapreduce.DataTransformMapper;
import com.cloudera.bigdata.analysis.dataload.mapreduce.LineRowMapper;
import com.cloudera.bigdata.analysis.dataload.source.DataSource;
import com.cloudera.bigdata.analysis.dataload.util.Util;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;

/**
 * DataLoad is a file-based loading tool for pseudo real-time batch loading.
 */
public class DataLoad {
  private static final Logger LOG = LoggerFactory.getLogger(DataLoad.class);
  private static final String USAGE = "Usage: com.cloudera.bigdata.analysis.dataload.DataLoad <property file>";
  private static final String JOB_NAME_PREFIX = "DATALOAD_";

  private static Configuration conf;
  private String propertyFolder;
  private Job job;
  private JobClient jobClient;

  public DataLoad(String propertyFile) {
    try {
      Properties props = new Properties();
      File file = new File(propertyFile);
      propertyFolder = file.getAbsoluteFile().getParent();
      props.load(new FileInputStream(file));

      Util.mergeProperties(props, conf);
      conf.set(Constants.PROPERTY_FOLDER_KEY, propertyFolder);
      if (conf.getBoolean(Constants.BUILD_INDEX, false)
          && !IndexUtil.isIndexConfAvailableForLoad(conf
              .get(Constants.HBASE_TARGET_TABLE_NAME))) {
        ETLException
            .handle("Failed to load data because there is index configuration error!");
      }
    } catch (FileNotFoundException e) {
      LOG.error("", e);
    } catch (IOException e) {
      LOG.error("", e);
    }

    preCheck();
  }

  public DataLoad(Configuration configuration) {
    conf = configuration;
  }

  private void preCheck() {
    if (Constants.DATALOAD_MAPRED_MODE.equalsIgnoreCase(conf
        .get(Constants.DATALOAD_MODE))
        && conf.getBoolean(Constants.STREAMING_FETCH_KEY, false)) {
      LOG.error("Streaming fetch is not supported in mapred mode");
      System.exit(1);
    }
  }

  public void start() {
    if (Constants.DATALOAD_CLIENT_MODE.equalsIgnoreCase(conf
        .get(Constants.DATALOAD_MODE))) {
      singleClientLoad();
    } else {
      mapredLoad();
    }
  }

  private void singleClientLoad() {
    try {
      int fetchParallel = conf.getInt(Constants.FETCH_PARALLEL_KEY,
          Constants.DEFAULT_FETCH_PARALLEL);
      int workerThreadsPerFetch = conf.getInt(Constants.THREADS_PER_MAPPER_KEY,
          Constants.DEFAULT_THREADS_PER_MAPPER);
      int numWorkers = fetchParallel * workerThreadsPerFetch;

      LoadTask task = new LoadTask(conf, fetchParallel, numWorkers);
      task.submitJob();
    } catch (IOException e) {
      LOG.error("", e);
    }
  }

  private void mapredLoad() {
    try {
      job = Job.getInstance(conf);
      // String jobTracker = conf.get(Constants.MAPRED_JOB_TRACKER_KEY);
      // int jobTrackerPort = conf.get(Constants.MAPRED_JOBTRACKER_PORT_KEY) ==
      // null ? Constants.DEFAULT_JOBTRACKER_PORT
      // : Integer.parseInt(conf.get(Constants.MAPRED_JOBTRACKER_PORT_KEY));
      // LOG.debug("jobttracker port :" + jobTrackerPort);
      // jobClient = new JobClient(NetUtils.createSocketAddr(jobTracker,
      // jobTrackerPort), new Configuration());

      DataSource dataSource = Util.newDataSource(conf);
      List<FileObject> files = Util.getFileList(dataSource);
      if (files == null || files.isEmpty()) {
        LOG.debug("No files are found");
        System.exit(0);
      }
      LOG.info("Got " + files.size() + " files to handle.");

      // Write FileObject list to HDFS
      FileSystem fileSystem = FileSystem.get(conf);
      String uuid = UUID.randomUUID().toString();
      String jobName = JOB_NAME_PREFIX + uuid;
      LOG.info("Job Name: " + jobName);

      Path tempPath = new Path("/tmp");

      Path inputPath = null;
      Path outputPath = new Path(tempPath, jobName + "_O");
      if (!conf.getBoolean(Constants.DATALOAD_ONLY_A_LARGE_FILE_KEY, false)) {
        inputPath = new Path(tempPath, jobName);
        FileInputFormat.setInputPaths(job, inputPath);
        FileObjectArrayWritable arrayWritable = new FileObjectArrayWritable(
            files, files.get(0).getClass());
        FSDataOutputStream outputStream = fileSystem.create(inputPath);
        arrayWritable.write(outputStream);
        outputStream.close();
      } else {
        // there is one and only one larger file(larger than HDFS block size) in
        // HDFSDIRS
        inputPath = new Path(conf.get(Constants.HDFSDIRS));
      }

      // configure job
      configureJob(jobName, inputPath, outputPath, files.size(), files.get(0)
          .getClass());

      job.waitForCompletion(true);
    } catch (Exception e) {
      LOG.error("", e);
    }
  }

  protected void configureJob(String jobName, Path inPath, Path outPath,
      int fileNum, Class<? extends FileObject> clazz) throws IOException {
    int fetchParallel = conf.getInt(Constants.FETCH_PARALLEL_KEY,
        Constants.DEFAULT_FETCH_PARALLEL);
    int fileNumPerMap = (fileNum + fetchParallel - 1) / fetchParallel;
    Configuration conf = job.getConfiguration();
    // DistributedCache.addArchiveToClassPath(new Path("/user/cluster.jar"),
    // conf);
    conf.setInt(Constants.FETCH_PARALLEL_KEY, fetchParallel);
    conf.set(Constants.FILEOBJECT_CLASS_KEY, clazz.getName());
    conf.setInt(Constants.LINES_PER_MAP_KEY, fileNumPerMap);
    LOG.debug(Constants.LINES_PER_MAP_KEY + " :" + fileNumPerMap);
    conf.set(Constants.TASK_TIMEOUT_KEY, "0");
    conf.set(Constants.MAPPER_SPECULATIVE_KEY, "false");
    conf.set(Constants.REDUCER_SPECULATIVE_KEY, "false");
    // conf.set("mapreduce.framework.name", "yarn");

    // currently, we only put one instance doc in DistributedCache
    String instanceDocPath = conf.get(Constants.INSTANCE_DOC_PATH_KEY);
    try {
      DistributedCache.addCacheFile(
          new URI(conf.get(Constants.FS_DEFAULT_NAME_KEY)
              + conf.get(Constants.INSTANCE_DOC_PATH_KEY)), conf);
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String fileName = instanceDocPath.substring(instanceDocPath
        .lastIndexOf('/') + 1);
    conf.set(Constants.INSTANCE_DOC_NAME_KEY, fileName);
    String qualifiedPath = conf.get(Constants.PROPERTY_FOLDER_KEY) + "/"
        + instanceDocPath;
    // qualifiedPath = Util.makeQualified(qualifiedPath, conf);
    LOG.debug("instanceDocPath :" + qualifiedPath);
    // conf.set("tmpfiles", qualifiedPath);
    job.setJobName(jobName);
    job.setJarByClass(DataTransformMapper.class);
    if (!conf.getBoolean(Constants.DATALOAD_ONLY_A_LARGE_FILE_KEY, false)) {
      DataFileInputFormat.setInputPaths(job, inPath);
      job.setInputFormatClass(DataFileInputFormat.class);
      job.setMapperClass(DataTransformMapper.class);
      job.setJarByClass(DataTransformMapper.class);
    } else {
      // there is one and only one larger file(larger than HDFS block size) in
      // HDFSDIRS
      TextInputFormat.setInputPaths(job, inPath);
      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(LineRowMapper.class);
      job.setJarByClass(LineRowMapper.class);
    }
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, outPath);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
  }

  public static void main(String[] args) throws IOException {
    conf = HBaseConfiguration.create();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (args.length != 1) {
      printUsage();
    }

    new DataLoad(args[0]).start();
  }

  private static void printUsage() {
    System.err.println(USAGE);
    System.exit(1);
  }
}
