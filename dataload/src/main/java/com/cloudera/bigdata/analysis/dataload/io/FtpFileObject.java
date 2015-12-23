package com.cloudera.bigdata.analysis.dataload.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;

public class FtpFileObject implements FileObject {
  private static final Logger LOG = LoggerFactory
      .getLogger(FtpFileObject.class);

  private String fileName;
  private long fileSize;

  private String server;
  private int port;
  private String userName;
  private String passwd;
  private String path;

  private String hostName;

  public FtpFileObject() {

  }

  public FtpFileObject(String ftpServer, int ftpPort, String ftpUser,
      String ftpPasswd, String ftpDir, String fileName) {
    this.server = ftpServer;
    this.port = ftpPort;
    this.userName = ftpUser;
    this.passwd = ftpPasswd;
    this.path = ftpDir;
    this.fileName = fileName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(server);
    out.writeInt(port);
    out.writeUTF(userName);
    out.writeUTF(passwd);
    out.writeUTF(path);
    out.writeUTF(fileName);
    if (!StringUtils.isEmpty(hostName)) {
      out.writeBoolean(true);
      out.writeUTF(hostName);
    } else {
      out.writeBoolean(false);
    }
    out.writeLong(fileSize);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    server = in.readUTF();
    port = in.readInt();
    userName = in.readUTF();
    passwd = in.readUTF();
    path = in.readUTF();
    fileName = in.readUTF();
    boolean flag = in.readBoolean();
    if (flag) {
      hostName = in.readUTF();
    }
    fileSize = in.readLong();
  }

  @Override
  public int compareTo(FileObject o) {
    if (!(o instanceof FtpFileObject)) {
      LOG.debug("Found an incorrect object: " + o.getClass().getName());
      return 1;
    }
    FtpFileObject ftpFileObject = (FtpFileObject) o;

    if (this.server.equals(ftpFileObject.getServer())
        && this.port == ftpFileObject.getPort()
        && this.userName.equals(ftpFileObject.getUserName())
        && this.passwd.equals(ftpFileObject.getPasswd())
        && this.path.equals(ftpFileObject.getPath())
        && this.fileName.equals(ftpFileObject.getName())
        && this.fileSize == ftpFileObject.getFileSize()) {
      return 0;
    }

    return -1;
  }

  public String getServer() {
    return server;
  }

  public int getPort() {
    return port;
  }

  public String getUserName() {
    return userName;
  }

  public String getPasswd() {
    return passwd;
  }

  public String getPath() {
    return path;
  }

  public long getFileSize() {
    return fileSize;
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public long getSize() {
    return fileSize;
  }

  @Override
  public String getCanonicalPath() {
    return path + "/" + fileName;
  }

  @Override
  public String getHostname() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public void setSize(long size) {
    this.fileSize = size;
  }

  /**
   * Get the table name from the file name. If the file name does not contain
   * the split char ":", the whole file name will be treated as table name. If
   * the file name contains the split char ":" and the format is
   * "tablename:startkey:endkey:numregions", it will return the array
   * {tablename, startkey, endkey, numregions}. In other case, such as
   * "temp1:temp2" or "temp1:temp2:temp3:temp4:temp5", it will only return the
   * string before the first ":", that is , {temp1}.
   * */
  public String[] getTableNameFromFileName() {
    if (fileName.indexOf(Constants.FTP_SPLIT) == -1) {
      String[] tableDescs = new String[1];
      // This file only includes table name, does not need the split keys
      tableDescs[0] = fileName;
      LOG.info("Get the table name " + tableDescs[0] + " from the file "
          + fileName + " : " + fileName);
      return tableDescs;
    } else {
      String[] tableDescs = fileName.split(Constants.FTP_SPLIT);
      if (tableDescs.length != 4) {
        String[] tempTableDescs = new String[1];
        tempTableDescs[0] = tableDescs[0];
        LOG.info("The format of file's name is not like \"tablename:startkey:endkey:numregions\", "
            + "so only get the first string as the table name :"
            + tempTableDescs[0]);
        return tempTableDescs;
      }
      return tableDescs;
    }
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass().getSimpleName() + "->" + "[" + server + ":"
        + port + ":" + userName + ":" + passwd + ":" + path + "/" + fileName
        + "]");
    return builder.toString();
  }
}
