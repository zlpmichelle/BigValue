package com.cloudera.bigdata.analysis.dataload.source;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.io.FtpFileObject;

@SuppressWarnings("rawtypes")
public class FtpDirDesc implements Comparable {
  public static Log LOG = LogFactory.getLog(FtpDirDesc.class);

  // Some properties of the ftp
  private String server;
  private int port;
  private String user;
  private String password;
  private String path;
  private String fileName;// if fileName==null, then this indicates an Ftp Dir

  public FtpDirDesc() {
    this.server = "";
    this.port = -1;
    this.user = "";
    this.password = "";
    this.path = "";
    this.fileName = "";
  }

  public FtpDirDesc(String server, int port, String user, String password,
      String path, String fileName) {
    this.server = server;
    this.port = port;
    this.user = user;
    this.password = password;
    this.path = path;
    this.fileName = fileName;
  }

  public FtpDirDesc(FtpFileObject ftpFileObject) {
    if (ftpFileObject != null) {
      this.server = ftpFileObject.getServer();
      this.port = ftpFileObject.getPort();
      this.user = ftpFileObject.getUserName();
      this.password = ftpFileObject.getPasswd();
      this.path = ftpFileObject.getPath();
    }
  }

  public FtpDirDesc(String ftpDir) {
    LOG.debug("########### ftpDir: " + ftpDir);
    String[] ftpDirDescArray = ftpDir.split(Constants.FTP_SPLIT);

    LOG.debug("########### ftpDirDescArray size: " + ftpDirDescArray.length);
    if (ftpDirDescArray.length == 5) {
      this.fileName = null;
    } else if (ftpDirDescArray.length == 6) {
      this.fileName = ftpDirDescArray[5].substring(0,
          ftpDirDescArray[5].lastIndexOf("]"));
    } else {
      // If the ftpDirs's split array's length is larger than 6
      // The argument is not valid
      LOG.error("The argument ftpDirs in the properties file is not valid, please check it.");
      System.exit(1);
    }
    LOG.debug(" ########### fileName: " + fileName);
    this.server = ftpDirDescArray[0];
    LOG.debug(" ########### server: " + server);
    this.port = Integer.parseInt(ftpDirDescArray[1]);
    LOG.debug(" ########### port: " + port);
    this.user = ftpDirDescArray[2];
    LOG.debug(" ########### user: " + user);
    this.password = ftpDirDescArray[3];
    LOG.debug(" ########### password: " + password);
    this.path = ftpDirDescArray[4];
    LOG.debug(" ########### path: " + path);
  }

  public FtpDirDesc(String ftpDir, boolean afterMap) {
    LOG.debug("########### ftpDir: " + ftpDir);
    String[] ftpDirDescArray = ftpDir.split(Constants.FTP_SPLIT);

    LOG.debug(" after map ########### ftpDirDescArray size: "
        + ftpDirDescArray.length);
    if (ftpDirDescArray.length == 6) {
      this.fileName = null;
    } else if (ftpDirDescArray.length == 7) {
      this.fileName = ftpDirDescArray[6].substring(0,
          ftpDirDescArray[6].lastIndexOf("]"));
    } else {
      // If the ftpDirs's split array's length is larger than 6
      // The argument is not valid
      LOG.error("The argument ftpDirs in the properties file is not valid, please check it.");
      System.exit(1);
    }
    LOG.debug(" after map ########### fileName: " + fileName);

    this.server = ftpDirDescArray[1].substring(2);
    LOG.debug(" after map ########### server: " + server);
    this.port = Integer.parseInt(ftpDirDescArray[2]);
    LOG.debug(" after map ########### port: " + port);
    this.user = ftpDirDescArray[3];
    LOG.debug(" after map ########### user: " + user);
    this.password = ftpDirDescArray[4];
    LOG.debug(" after map ########### password: " + password);
    this.path = ftpDirDescArray[5];
    LOG.debug(" after map ########### path: " + path);
  }

  // The getter and setter
  public String getServer() {
    return this.server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUser() {
    return this.user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPath() {
    return this.path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getFileName() {
    return this.fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setFileFullPath(String filePath) {
    int lastSep = filePath.lastIndexOf('/');
    this.path = filePath.substring(0, lastSep);
    this.fileName = filePath.substring(lastSep + 1);
  }

  public boolean isSameConnection(FtpDirDesc ftp) {
    return (this.server.equals(ftp.server) && this.port == ftp.port
        && this.user.equals(ftp.user) && this.password.equals(ftp.password));
  }

  @Override
  public int compareTo(Object arg0) {
    if (arg0 == null) {
      throw new RuntimeException("This FTPDirDesc object doesn't exist.");
    }
    FtpDirDesc arg = (FtpDirDesc) arg0;
    if ((this.server.equals(arg.server)) && (this.port == arg.port)
        && (this.user.equals(arg.user)) && (this.password.equals(arg.password))
        && (this.path.equals(arg.path)))
      return 0;
    return -1;
  }

  public String toString() {
    String fn = "";
    if (this.fileName != null) {
      fn = ":" + this.fileName;
    }
    String ret = "[ftp://" + server + ":" + port + ":" + user + ":" + password
        + ":" + path + fn + "]";
    return ret;
  }
}
