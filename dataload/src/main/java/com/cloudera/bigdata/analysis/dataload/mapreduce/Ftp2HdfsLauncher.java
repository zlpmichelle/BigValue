package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.bigdata.analysis.dataload.io.FixedNLineInputFormat;
import com.cloudera.bigdata.analysis.dataload.source.FtpDirDesc;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;
import com.cloudera.bigdata.analysis.dataload.util.FtpUtil;

public class Ftp2HdfsLauncher implements LoadLauncher {
  static final String USAGE_STR = "FtpToHdfsLauncher <properties_file>";
  static final Log LOG = LogFactory.getLog(Ftp2HdfsLauncher.class);

  static String JOB_NAME_PREFIX = "DATALOAD_FTPTOHDFS";

  private String mapredServer;
  private int mapredPort;
  private FtpDirDesc[] ftpDirs;
  private boolean recursiveList;
  private int connNum;
  private String jobName;
  private String jobQueueName;
  private int linePerSplit;
  private ArrayList<Job> jobList;
  private Path output;
  private Path ftpListFile = null;
  private boolean deleteFiles;
  private boolean moveFiles;

  public Ftp2HdfsLauncher(String jobName, Path output) {
    this.jobName = jobName;
    this.jobList = new ArrayList<Job>();
    this.output = output;
  }

  public Ftp2HdfsLauncher(String jobName, String output) {
    this(jobName, new Path(output));
  }

  /**
   * Get all configuration from config file.
   * 
   * @param props
   *          - Configuration properties.
   * @return True if configuration is successful.
   */
  public boolean conf(Properties props) {
    try {
      mapredServer = props.getProperty("mapredServer");
      mapredPort = Integer.parseInt(props.getProperty("mapredPort"));
      jobQueueName = props.getProperty("jobQueueName", "default");
      deleteFiles = Boolean.parseBoolean(props.getProperty("deleteFiles",
          "false"));
      moveFiles = Boolean.parseBoolean(props.getProperty("moveFiles", "false"));

      String[] ftpDirsString = props.getProperty("ftpDirs").split(",");

      ftpDirs = new FtpDirDesc[ftpDirsString.length];
      for (int i = 0; i < ftpDirsString.length; i++) {
        ftpDirs[i] = new FtpDirDesc(ftpDirsString[i]);
      }

      recursiveList = Boolean.parseBoolean(props.getProperty("recursiveList",
          "false"));
      connNum = Integer.parseInt(props.getProperty("connectionPerServer", "1"));
    } catch (RuntimeException e) {
      LOG.error("Failed to parse configuration.", e);
      return false;
    }

    return true;
  }

  /**
   * Submit the Map-Reduce job.
   * 
   * @param conf
   *          - The configuration of the Map-Reduce job.
   * @param jobName
   *          - The name of the job.
   * @param inputPath
   *          - Input of the mapred job.
   * @param outputPath
   *          - Output of the mapred job.
   * @return The job that is submitted to hadoop.
   * @throws Exception
   */
  public Job submitJob(Configuration conf, String jobName, Path inputPath,
      Path outputPath) throws Exception {
    LOG.info("Submitting job " + jobName);

    Job job = new Job(conf, jobName);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(Ftp2HdfsMapper.class);
    job.setMapperClass(Ftp2HdfsMapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(FixedNLineInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    FixedNLineInputFormat.setNumLinesPerSplit(job, linePerSplit);
    FileInputFormat.setInputPaths(job, inputPath);
    job.submit();

    return job;
  }

  /**
   * Scan all files in the ftp server that need to be transferred.
   * 
   * @return Hashmap to present files per server.
   */
  public HashMap<String, List<String>> scanTargetFiles() {
    LOG.info("Scanning all possible file paths.");
    HashMap<String, List<String>> filePerServer = new HashMap<String, List<String>>();
    List<String> files = null;

    for (FtpDirDesc ftpDirDesc : ftpDirs) {
      // Scan files under one ftp directory.
      LOG.info("Scan directory " + ftpDirDesc.toString());

      files = scanOneDir(ftpDirDesc);

      if (files == null) {
        continue;
      } else if (files.size() == 0) {
        continue;
      }

      if (!filePerServer.containsKey(ftpDirDesc.getServer())) {
        filePerServer.put(ftpDirDesc.getServer(), files);
      } else {
        filePerServer.get(ftpDirDesc.getServer()).addAll(files);
      }
    }

    LOG.info("Finish scanning.");

    return filePerServer;
  }

  /**
   * Scan files under one ftp directory.
   * 
   * @param dir
   *          - The ftp directory to scan.
   * @return List of paths of target file.
   */
  public List<String> scanOneDir(FtpDirDesc dir) {
    FtpUtil ftpUtil = new FtpUtil();
    List<String> ret = new ArrayList<String>();
    List<String> files;

    LOG.info("Connecting to FTP Server " + dir.getServer() + ":"
        + dir.getPort());
    try {
      ftpUtil.connectServer(dir.getServer(), dir.getPort(), dir.getUser(),
          dir.getPassword(), "");

      if (recursiveList) {
        files = ftpUtil.getFileListRecursive(dir.getPath(), null, ".ERR");

      } else {
        files = ftpUtil.getFileList(dir.getPath(), null, ".ERR");
      }

      LOG.info(dir.getPath() + " : " + files.size() + " files.");

      ftpUtil.closeServer();

    } catch (SocketException e) {
      LOG.error(
          "Fail to connect FTP Server " + dir.getServer() + ":" + dir.getPort(),
          e);
      return null;
    } catch (IOException e) {
      LOG.error("Fail to read from " + dir.getServer() + ":" + dir.getPort(), e);
      e.printStackTrace();
      return null;
    }

    int i = 0;
    // Add the full info of ftp server:port:user:pwd:dir
    for (String file : files) {
      Path filePath = new Path(file);
      LOG.debug("##################### " + i + ", filePath: " + filePath);
      LOG.debug("##################### " + i + ", dir: " + dir);
      dir.setFileName(filePath.getName());
      ret.add(dir.toString());
    }
    dir.setFileName(null);

    return ret;
  }

  /**
   * Launch the job to finish the data load task.
   * 
   * @return Map-Reduce tasks that is submitted.
   */
  public List<Job> launch() throws IOException {
    LOG.info("Launch jobs load file from FTP to HDFS.");

    // Create client to get jobtracker information.
    JobClient client;
    InetSocketAddress jobTrackerAddr = new InetSocketAddress(mapredServer,
        mapredPort);
    try {
      client = new JobClient(jobTrackerAddr, new Configuration());
    } catch (IOException e) {
      LOG.error("Failed to create mapred job client for server " + mapredServer
          + ":" + mapredPort, e);
      return null;
    }

    int serverIdx = 0;
    HashMap<String, List<String>> filePerServer = scanTargetFiles();
    Set<String> servers = filePerServer.keySet();

    // Get tmp directory.
    String pathString = CommonUtils.getTempDir(client);

    // Get file system.
    FileSystem fs;
    try {
      fs = client.getFs();
    } catch (IOException e) {
      LOG.error("Failed to get file system.", e);
      return null;
    }

    // Create output path if the path does not exist.
    try {
      if (!fs.exists(output)) {
        fs.mkdirs(output);
      } else if (fs.isFile(output)) {
        LOG.error("Failed to create output folder " + output.toString()
            + ". Target path is a file.");
        return null;
      }
    } catch (IOException e) {
      LOG.error("Failed to create output folder " + output.toString(), e);
      return null;
    }

    LOG.info("Writing files to " + output.toString());
    for (String server : servers) {
      // Create file containing file list in the ftp server.
      LOG.info("Writing file in server " + server + " to input path for job "
          + jobName);

      // Write file list to the input path of MR job.
      ftpListFile = new Path(pathString, jobName + "_" + serverIdx++);

      try {
        if (!fs.exists(ftpListFile.getParent())) {
          fs.mkdirs(ftpListFile.getParent());
        }

        LOG.info("Full input path: " + ftpListFile.toString());

        FSDataOutputStream os = fs.create(ftpListFile, true, 1024);

        byte[] newline = "\n".getBytes("UTF-8");

        List<String> inputLines = filePerServer.get(server);
        // Calculate lines per split. The value should be at least 1.
        linePerSplit = Math.max(inputLines.size() / connNum, 1);

        for (String inputLine : inputLines) {
          Text to = new Text(inputLine);
          os.write(to.getBytes(), 0, to.getLength());
          os.write(newline);
        }
        os.close();
      } catch (IOException e) {
        LOG.error("Failed to write to file " + ftpListFile.toUri(), e);
        continue;
      }

      LOG.info("Finished writing mapreduce input file.");

      // Submit MR job.
      try {
        // JobConf conf = new JobConf(client.getConf());
        // JobConf conf = new JobConf(new Configuration());
        Configuration conf = client.getConf();
        // conf.setQueueName(jobQueueName);
        conf.set("fs.defaultFS", client.getFs().getUri().toString());
        conf.set("mapreduce.job.tracker", jobTrackerAddr.getHostName() + ":"
            + jobTrackerAddr.getPort());
        conf.set("mapred.job.queue.name", jobQueueName);
        conf.setLong("mapred.task.timeout", 0);
        conf.set("mapper.fileOutputPath", output.toString());
        conf.setBoolean("mapper.deleteFiles", deleteFiles);
        conf.setBoolean("mapper.moveFiles", moveFiles);

        DistributedCache.addArchiveToClassPath(new Path("/user/cluster.jar"),
            conf);

        Job newJob = submitJob(conf, jobName, ftpListFile, output);

        if (newJob != null) {
          jobList.add(newJob);
        } else {
          LOG.error("Failed to submit job " + jobName);
        }
      } catch (Exception e) {
        LOG.error("Failed to submit job " + jobName, e);
      }

      serverIdx++;
    }

    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    return jobList;
  }

  /**
   * Check if the MR jobs are completed.
   * 
   * @return True if all jobs are completed.
   * @throws IOException
   */
  public boolean isComplete() throws IOException, InterruptedException {
    if (jobList == null) {
      return true;
    } else {
      for (Job job : jobList) {
        if (!job.isComplete()) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * Delete the intermediary files.
   * 
   * @return True if success.
   * @throws IOException
   */
  public void cleanup() throws IOException {
    LOG.info("Deleting tmp file " + ftpListFile);

    JobClient client;
    client = new JobClient(new InetSocketAddress(mapredServer, mapredPort),
        new Configuration());
    FileSystem fs = client.getFs();
    fs.delete(ftpListFile, true);
  }

  /**
   * Get submitted MR jobs.
   * 
   * @return List of MR jobs.
   */
  public List<Job> jobList() {
    return jobList;
  }

  /**
   * Get output path storing all files from ftp.
   * 
   * @return Path storing all files from ftp
   */
  public Path getOutputPath() {
    return output;
  }

  @Override
  public void waitForComplete() throws IOException, InterruptedException {
    // Wait for job to complete.
    while (!isComplete()) {
      try {
        // Release occupied resource.
        Thread.sleep(1);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted.", e);
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    if (args.length < 1) {
      System.out.print(USAGE_STR);
      return;
    }

    Properties props = new Properties();
    try {
      props.load(new FileInputStream(args[0]));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    String mapredServer = props.getProperty("mapredServer");
    int mapredPort = Integer.parseInt(props.getProperty("mapredPort"));

    String pathString;
    try {
      JobClient client = new JobClient(new InetSocketAddress(mapredServer,
          mapredPort), new Configuration());
      pathString = CommonUtils.getTempDir(client);
      client.close();
    } catch (IOException e) {
      LOG.error(
          "Failed to get tmp dir from " + mapredServer + ":" + mapredPort, e);
      return;
    }

    DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    String jobName = JOB_NAME_PREFIX;// + UUID.randomUUID().toString()
    String hdfsDirs = props.getProperty("hdfsDirs");
    // + dateFormat.format(new Date());
    Path output = new Path(hdfsDirs, jobName);

    Ftp2HdfsLauncher launcher = new Ftp2HdfsLauncher(jobName, output);
    launcher.conf(props);

    LOG.info("Launching Data Tranform job...");

    List<Job> jobList = launcher.launch();

    while (!launcher.isComplete()) {
      Thread.sleep(1000);
    }

    // launcher.cleanup();
  }
}
