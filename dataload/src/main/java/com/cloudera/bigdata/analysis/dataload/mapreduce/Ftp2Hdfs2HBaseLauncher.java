package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.bigdata.analysis.dataload.DataLoad;
import com.cloudera.bigdata.analysis.dataload.source.FtpDirDesc;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;
import com.cloudera.bigdata.analysis.dataload.util.Util;
import com.intel.fangpei.for_test_code.DisLog;
import com.intel.fangpei.for_test_code.DisLogObj;
import com.intel.fangpei.network.PacketLine.segment;

public class Ftp2Hdfs2HBaseLauncher implements LoadLauncher {

  static final String USAGE_STR = "FtpToHbaseLauncher <properties_file>";
  static final Log LOG = LogFactory.getLog(Ftp2Hdfs2HBaseLauncher.class);

  static String JOB_NAME_PREFIX = "DATALOAD_FTPTOHBASE_";

  private String mapredServer;
  private int mapredPort;
  private FtpDirDesc[] ftpDirs;
  private long launchPeriod;
  private boolean isRunning;
  private JobClient client;
  private Properties props;
  private HashMap<String, LoadLauncher> fileMap;
  private Configuration conf;

  public Ftp2Hdfs2HBaseLauncher() {
    isRunning = false;
    fileMap = new HashMap<String, LoadLauncher>();
    conf = new Configuration();

    try {
      conf.addResource((new File("/etc/hbase/conf/hbase-site.xml")).toURI()
          .toURL());
      conf.addResource((new File("/etc/hadoop/conf/core-site.xml")).toURI()
          .toURL());
      conf.addResource((new File("/etc/hadoop/conf/hdfs-site.xml")).toURI()
          .toURL());
      conf.addResource((new File("/etc/hadoop/conf/mapred-site.xml")).toURI()
          .toURL());
      conf.reloadConfiguration();
    } catch (MalformedURLException e) {
    }
  }

  public Ftp2Hdfs2HBaseLauncher(Properties props) throws IOException {
    isRunning = false;
    this.props = props;
    conf(props);
    this.isRunning = false;
    conf = new Configuration();
  }

  /**
   * Get all configuration from config file.
   * 
   * @param props
   *          - Configuration properties.
   * @return True if configuration is successful.
   */
  public boolean conf(Properties props) {
    this.props = props;
    try {
      mapredServer = props.getProperty("mapredServer");
      mapredPort = Integer.parseInt(props.getProperty("mapredPort"));
      launchPeriod = Integer.parseInt(props.getProperty("launchPeriod", "300")) * 1000;

      String[] ftpDirsString = props.getProperty("ftpDirs").split(",");

      ftpDirs = new FtpDirDesc[ftpDirsString.length];
      for (int i = 0; i < ftpDirsString.length; i++) {
        ftpDirs[i] = new FtpDirDesc(ftpDirsString[i]);
      }

      client = new JobClient(new InetSocketAddress(mapredServer, mapredPort),
          new Configuration());
    } catch (RuntimeException e) {
      LOG.error("Failed to parse configuration.", e);
      return false;
    } catch (IOException e) {
      LOG.error("Failed to connect to server " + mapredServer + ":"
          + mapredPort + ".", e);
      return false;
    }

    return true;
  }

  public long getLaunchPeriod() {
    return launchPeriod;
  }

  public void setLaunchPeriod(int launchPeriod) {
    this.launchPeriod = launchPeriod;
  }

  public void stop() {
    isRunning = false;
  }

  public void moveToTarger(Path ftpToHdfsOutputPath)
      throws InterruptedException {

    for (String path : fileMap.keySet()) {
      try {
        LoadLauncher launcher = fileMap.get(path);

        if (launcher == null) {
          handleIdlePath(path, ftpToHdfsOutputPath);
        } else if (launcher.isComplete()) {
          handleIdlePath(path, ftpToHdfsOutputPath);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to get job information of file " + path, e);
      }
    }

  }

  public void handleIdlePath(String dir, Path ftpToHdfsOutputPath)
      throws IOException {
    FileSystem fs = client.getFs();
    Path path = new Path(dir);
    FileStatus[] files = fs.listStatus(path);

    if (files.length != 0) {
      for (FileStatus file : files) {
        fs.rename(file.getPath(), new Path(ftpToHdfsOutputPath, file.getPath()
            .getName()));
      }
    } else {
      fs.delete(path, true);
    }

    fileMap.remove(dir);
  }

  public List<String> getIdleDir(Map<String, LoadLauncher> fileMap)
      throws InterruptedException {
    List<String> idle = new ArrayList<String>();

    for (String file : fileMap.keySet()) {
      try {
        LoadLauncher launcher = fileMap.get(file);
        if (launcher == null) {
          idle.add(file);
        } else if (launcher.isComplete()) {
          idle.add(file);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to get job information of file " + file, e);
      }
    }

    return idle;
  }

  public void sleep(long runningTime) {

    if (runningTime < launchPeriod) {
      try {
        Thread.sleep(launchPeriod - runningTime);
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  /**
   * Launch the MR jobs to download ftp files to hbase.
   * 
   * @return The submitted jobs.
   * @throws IOException
   */
  public List<Job> launch() throws IOException, InterruptedException {
    List<Job> jobs = new ArrayList<Job>();
    if (acquireLock()) {
      LOG.info("Launnhing job to load date from ftp to hbase.");
      // Collect info to name jobs.
      DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

      String jobName = JOB_NAME_PREFIX + dateFormat.format(new Date());
      LOG.info("Job name: " + jobName);

      // client.getJobsFromQueue("default");
      if (checkRunningJob()) {
        return jobs;
      }

      // The tmp dir used to store files from ftp to hdfs.
      Path tmpDir = new Path(getTmpDir(jobName + "DIR"));
      Path outDir = new Path(getTmpDir(jobName + "OUT"));

      // Load data from ftp to hdfs.
      Ftp2HdfsLauncher ftp2HdfsJob = new Ftp2HdfsLauncher(jobName, tmpDir);
      ftp2HdfsJob.conf(props);
      jobs.addAll(ftp2HdfsJob.launch());

      try {
        ftp2HdfsJob.waitForComplete();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to fetch job information of job hdfs-to-hbase "
            + jobName + ".", e);
      }

      LOG.info("Complete ftp-to-hdfs job " + jobName
          + ". Cleaning up tmp files.");
      try {
        ftp2HdfsJob.cleanup();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to clean up remaining tmp files of " + jobName, e);
      }

      // Get all files that should be handled.
      // HdfsToHbaseLauncher hdfs2Hbase = new HdfsToHbaseLauncher(jobName,
      // tmpDir,outDir);
      // hdfs2Hbase.conf(props);
      // jobs.addAll(hdfs2Hbase.launch());
      // try {
      // hdfs2Hbase.waitForComplete();
      // } catch (IOException e) {
      // LOG.error("Failed to fetch job information of job hdfs-to-hbase "
      // + jobName + ".", e);
      // return jobs;
      // }
      // hdfs2Hbase.cleanup();
      props.setProperty("hdfsDirs", tmpDir.toString());
      try {
        Util.mergeProperties(props, conf);
        DataLoad loader = new DataLoad(conf);
        loader.start();
      } finally {

      }
      releaseLock();
    }
    return jobs;

  }

  public boolean checkRunningJob() {
    // client.getJobsFromQueue("default");
    try {
      JobStatus[] status = client.getAllJobs();

      for (JobStatus stat : status) {
        RunningJob runningJob = client.getJob(stat.getJobID());

        if (!runningJob.isComplete()) {
          String name = runningJob.getJobName();
          if (name.startsWith(JOB_NAME_PREFIX)) {
            LOG.error("One same job: " + name + " is already running.");
            return true;
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to fetch job information in current cluster.", e);
      LOG.error("May cause data error for given source.");
      return true;
    }

    return false;
  }

  /**
   * Generate tmp directory according to given job name.
   * 
   * @param jobName
   *          - The job name.
   * @return Files named as job name under tmp directory.
   */
  public String getTmpDir(String jobName) {
    // Get tmp dir of hadoop.
    String pathString = CommonUtils.getTempDir(client);

    if (pathString != null) {
      String tmpDir = pathString + "/" + jobName; // (new Path(pathString,
                                                  // jobName)).toString();
      return tmpDir;
    } else {
      return null;
    }

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

  @Override
  public void cleanup() throws IOException {

  }

  @Override
  public boolean isComplete() throws IOException, InterruptedException {
    if (isRunning) {
      return false;
    } else {
      Iterator<LoadLauncher> jobs = fileMap.values().iterator();

      while (jobs.hasNext()) {
        LoadLauncher launcher = jobs.next();

        if (!launcher.isComplete()) {
          return false;
        }
      }
    }

    return true;
  }

  @SuppressWarnings("unused")
  private boolean acquireLock() {
    try {
      String lockFileName = "/user/" + JOB_NAME_PREFIX + ".lck";
      LOG.info("Trying to acquire jobclient lock..." + lockFileName);
      Path lockFile = new Path(lockFileName);
      FileSystem fs = FileSystem.get(conf);
      fs.create(lockFile, false).close();
      fs.close();
    } catch (IOException e) {
      LOG.info("Failed to acquire jobclient lock...[Cause]" + e.getMessage());
      return false;
    }
    LOG.info("Successfully acquired jobclient lock.");
    return true;
  }

  @SuppressWarnings("unused")
  private boolean releaseLock() {
    try {
      String lockFileName = "/user/" + JOB_NAME_PREFIX + ".lck";
      LOG.info("Trying to release jobclient lock..." + lockFileName);
      Path lockFile = new Path(lockFileName);
      FileSystem fs = FileSystem.get(conf);
      boolean deleted = fs.delete(lockFile, false);
      fs.close();
      if (deleted) {
        LOG.info("Successfully released jobclient lock.");
        return true;
      }
      LOG.info("Failed to acquire jobclient lock...[Cause]Cannot delete "
          + JOB_NAME_PREFIX + ".lck");
      return false;
    } catch (IOException e) {
      LOG.info("Failed to release jobclient lock...[Cause]" + e.getMessage());
      return false;
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    Thread t = new Thread() {
      public void run() {
        // start server
        DisLog log = new DisLog();
        DisLogObj server = log.StartServer("4399");
        while (true) {
          try {
            FileWriter fw = new FileWriter("/tmp/testlogcollection.txt", true);
            segment se = server.receive();
            if (se != null) {
              fw.write("received:" + new String(se.p.getArgs()) + "\n");
            }
            fw.flush();
            fw.close();
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          } catch (IOException e1) {
            e1.printStackTrace();
          }
        }
      }
    };
    t.setDaemon(true);
    t.start();

    if (args.length < 1) {
      System.out.print(USAGE_STR);
      return;
    }
    Properties props = new Properties();
    try {
      props.load(new FileInputStream(args[0]));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Ftp2Hdfs2HBaseLauncher launcher = new Ftp2Hdfs2HBaseLauncher();
    if (launcher.conf(props)) {
      // launcher.periodicallylaunch(launcher.getLaunchPeriod());
      launcher.launch();
    }
    // launcher.launch();

  }
}
