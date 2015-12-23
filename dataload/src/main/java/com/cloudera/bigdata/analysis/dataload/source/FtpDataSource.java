package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
//import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.io.FtpFileObject;

//import com.cloudera.hbase.client.dataload.source.DataSource.FileHandleStatus;

public class FtpDataSource extends DataSource {
  public static Log LOG = LogFactory.getLog(FtpDataSource.class);
  static {
    DataSource.define(FtpDataSource.class, new FtpDataSource());
  }

  // The properties's name in the properties file
  private static final String FTPDIRS_SPLIT = ",";
  // private static final String FTP_SPLIT = ":";

  private FtpDirDesc[] ftpDirDescArray;

  private FTPClient[] ftpClientArray;
  // private Map<FTPDirDesc, FTPClient> ftpDirDescClientMap;

  @SuppressWarnings("unused")
  private static final String INPUT_ENCODING = "UTF-8";

  // set the FTPClient data timeout to 2 minutes
  private static int DATA_TIMEOUT = 120000;
  // set the FTPClient connection timeout to 2 minutes
  @SuppressWarnings("unused")
  private static int CONNECT_TIMEOUT = 120000;

  private static int RETRY_TIMES = 3;
  private static long RETRY_INTERVAL = 1 * 60 * 1000; // 1 minute

  public FtpDataSource() {
  }

  @Override
  public void init(Configuration conf) throws IOException {
    String ftpDirs = conf.get(Constants.DATALOAD_SOURCE_FTP_DIR);
    LOG.debug("ftpDirs in FtpDataSource init :" + ftpDirs);
    if (StringUtils.isEmpty(ftpDirs)) {
      // If the ftpDirs is empty
      LOG.error("Please set the ftpDirs in the properties fiels.");
      System.exit(1);
    }
    String[] ftpDirArray = ftpDirs.split(FTPDIRS_SPLIT);
    this.ftpDirDescArray = new FtpDirDesc[ftpDirArray.length];
    LOG.debug("ftpDirArray length in FtpDataSource init :" + ftpDirArray.length);
    for (int i = 0; i < ftpDirArray.length; i++) {
      FtpDirDesc ftpDirDesc = new FtpDirDesc(ftpDirArray[i]);
      LOG.debug(i + ": ftpDirDesc : " + ftpDirs.toString());
      ftpDirDescArray[i] = ftpDirDesc;
      LOG.info("Add new ftp dir desc : " + ftpDirDesc.toString());
    }

    // this.ftpClientArray = new FTPClient[ftpDirDescArray.length];
  }

  @Override
  public void connect() throws IOException {
    this.ftpClientArray = new FTPClient[ftpDirDescArray.length];
    LOG.debug("ftpDirDescArray length in FtpDataSource connect "
        + ftpDirDescArray.length);
    for (int i = 0; i < ftpDirDescArray.length; i++) {
      FtpDirDesc ftpDirDesc = ftpDirDescArray[i];
      FTPClient ftpClient = new FTPClient();
      if (!ftpClient.isConnected()) {
        ftpClient.connect(ftpDirDesc.getServer(), ftpDirDesc.getPort());
        LOG.info("Connecting to " + ftpDirDesc.getServer() + ":"
            + ftpDirDesc.getPort() + " ...");
        int reply = ftpClient.getReplyCode();
        LOG.info(reply);

        ftpClient.setDataTimeout(DATA_TIMEOUT);
        // After connection attempt, check the reply code to verify
        // success
        if (!FTPReply.isPositiveCompletion(reply)) {
          ftpClient.disconnect();
          LOG.error("FTP server " + ftpDirDesc.getServer()
              + " refused connection.");
          continue;
        }
        ftpClient.login(ftpDirDesc.getUser(), ftpDirDesc.getPassword());

      }
      ftpClientArray[i] = ftpClient;
    }

    LOG.debug("ftpClientArray length in FtpDataSource connect "
        + ftpClientArray.length);
  }

  @Override
  public void close() throws IOException {
    for (int i = 0; i < ftpClientArray.length; i++) {
      FTPClient ftpClient = ftpClientArray[i];
      FtpDirDesc ftpDirDesc = ftpDirDescArray[i];
      if (ftpClient != null) {
        try {
          if (ftpClient.isConnected()) {
            ftpClient.disconnect();
            LOG.info("Successfully close ftp server: " + ftpDirDesc.getServer()
                + ":" + ftpDirDesc.getPort());
          }
        } catch (IOException e) {
          LOG.error("Failed to close ftp server:" + ftpDirDesc.getServer()
              + ":" + ftpDirDesc.getPort() + "; Cause:" + e.getMessage());
        }
      }
    }
  }

  private boolean reconnect(FileObject file) {
    boolean flag = false;
    if (file != null) {
      FtpDirDesc ftpDirDesc = new FtpDirDesc((FtpFileObject) file);
      FTPClient ftpClient = getFtpClientBasedFile(file);
      if (ftpClient == null) {
        // Generally , this ftpClient shouldn't be null
        ftpClient = new FTPClient();
      }
      if (!ftpClient.isConnected()) {
        // Reconnect
        for (int i = 0; i < RETRY_TIMES; i++) {
          LOG.info(" FTP Client" + ftpDirDesc.toString()
              + " reconnect: Retried " + i + " times");
          try {
            Thread.sleep(RETRY_INTERVAL);

            ftpClient.connect(ftpDirDesc.getServer(), ftpDirDesc.getPort());
            LOG.info("Reconnecting to " + ftpDirDesc.getServer() + ":"
                + ftpDirDesc.getPort() + " ... Retried " + i + " times");
            int reply = ftpClient.getReplyCode();
            LOG.info(reply);
            ftpClient.setDataTimeout(DATA_TIMEOUT);
            // After connection attempt, check the reply code to verify
            // success
            if (!FTPReply.isPositiveCompletion(reply)) {
              ftpClient.disconnect();
              LOG.error("FTP server " + ftpDirDesc.getServer()
                  + " refused connection. Retried " + i + " times");
              continue;
            }
            ftpClient.login(ftpDirDesc.getUser(), ftpDirDesc.getPassword());
            if (ftpClient.isConnected()) {
              flag = true;
              break;
            }
          } catch (InterruptedException e) {
            LOG.warn("InterruptedException happened when reconnect.");
            e.printStackTrace();
          } catch (IOException e) {
            LOG.warn("IOException happened when reconnect");
            e.printStackTrace();
          }
        }
      }
    }
    return flag;
  }

  @Override
  public List<FileObject> getFileList() throws IOException {
    List<FileObject> retList = new ArrayList<FileObject>();
    LOG.info("ftpDirDescArray.length " + ftpDirDescArray.length);
    LOG.info("ftpClientArray.length " + ftpClientArray.length);
    for (int i = 0; i < ftpDirDescArray.length; i++) {
      retList.addAll(getFileList(ftpClientArray[i], ftpDirDescArray[i]));
    }
    return retList;
  }

  private List<FileObject> getFileList(FTPClient ftpClient,
      FtpDirDesc ftpDirDesc) throws IOException {
    FTPFile[] ftpFiles = ftpClient.listFiles(ftpDirDesc.getPath());
    LOG.info("Get the file array from the " + ftpDirDesc.getPath()
        + " , its length is " + ftpFiles.length);
    List<FileObject> retList = new ArrayList<FileObject>();
    if (ftpFiles == null || ftpFiles.length == 0) {
      return retList;
    }
    for (FTPFile ftpFile : ftpFiles) {
      if (ftpFile.isFile()) {
        FtpFileObject ftpFileObject = new FtpFileObject(ftpDirDesc.getServer(),
            ftpDirDesc.getPort(), ftpDirDesc.getUser(),
            ftpDirDesc.getPassword(), ftpDirDesc.getPath(), ftpFile.getName());
        ftpFileObject.setSize(ftpFile.getSize());
        retList.add(ftpFileObject);
      }
    }
    LOG.info("Get the file list from the " + ftpDirDesc.getPath()
        + " , its length is " + retList.size());
    return retList;
  }

  @Override
  public InputStream readFile(FileObject file) {
    if (file != null) {
      FtpDirDesc ftpDirDesc = new FtpDirDesc((FtpFileObject) file);
      LOG.info("Read file's dir desc : " + ftpDirDesc.toString());
      FTPClient ftpClient = getFtpClientBasedFile(file);
      InputStream is = null;
      try {
        LOG.info("Read file's dir desc :  ftpDirDesc.getPath() "
            + ftpDirDesc.getPath());
        LOG.info("Read file's dir desc :  file.getName() " + file.getName());
        ftpClient.changeWorkingDirectory(ftpDirDesc.getPath());
        is = ftpClient.retrieveFileStream(file.getName());
      } catch (IOException e) {
        LOG.warn("IOException happened when read file in the ftp data source");
        e.printStackTrace();
        is = null;
        if (reconnect(file)) {
          // If reconnect successfully, get the input stream again
          try {
            is = ftpClient.retrieveFileStream(file.getName());
          } catch (IOException e1) {
            LOG.warn("IOException happened when read file again after reconnect");
            // Failed again after reconnect successfully, set the inputstream
            // with null
            is = null;
            e1.printStackTrace();
          }
        }
      }
      return is;
    }
    return null;
  }

  @Override
  public void closeFile(FileObject file, InputStream is,
      FileHandleStatus fileHandleStatus, Exception e) throws IOException {

    if (is != null) {
      is.close();
      FTPClient ftpClient = getFtpClientBasedFile(file);
      ftpClient.completePendingCommand();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends FileObject> getFileObjectClass() {
    try {
      return (Class<? extends FileObject>) Class
          .forName("com.cloudera.bigdata.analysis.dataload.io.FtpFileObject");
    } catch (ClassNotFoundException e) {
      LOG.error("Class not found when get file object class");
      e.printStackTrace();
    }
    return null;
  }

  private FTPClient getFtpClientBasedFile(FileObject file) {
    FtpDirDesc ftpDirDesc = new FtpDirDesc((FtpFileObject) file);
    for (int i = 0; i < ftpDirDescArray.length; i++) {
      if (ftpDirDesc.compareTo(ftpDirDescArray[i]) == 0) {
        return ftpClientArray[i];
      }
    }
    return null;
  }

}
