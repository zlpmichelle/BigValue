package com.cloudera.bigdata.analysis.dataload.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import com.cloudera.bigdata.analysis.dataload.source.FtpDirDesc;

public class FtpUtil {
  static final Log LOG = LogFactory.getLog(FtpUtil.class);

  private FTPClient ftpClient;
  public static final int BINARY_FILE_TYPE = FTP.BINARY_FILE_TYPE;
  public static final int ASCII_FILE_TYPE = FTP.ASCII_FILE_TYPE;
  private String server;
  private int port;
  private String user;
  private String password;

  private static int RETRY_TIMES = 3;
  private static long RETRY_INTERVAL = 1 * 60 * 1000; // 1 minute
  private static int DATA_TIMEOUT = 120000;

  public void connectServer(String server, int port, String user,
      String password, String path) throws SocketException, IOException {
    this.server = server;
    this.port = port;
    this.user = user;
    this.password = password;
    ftpClient = new FTPClient();
    ftpClient.connect(server, port);
    // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
    // BasicMessage.OP_MESSAGE, "Connected to " + server + "."), 1);
    // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
    // BasicMessage.OP_MESSAGE, String.valueOf(ftpClient.getReplyCode())), 1);
    LOG.info("Connected to " + server + ".");
    // LOG.info("************" + ftpClient.getReplyCode());
    ftpClient.login(user, password);
    // Path is the sub-path of the FTP path
    if (path.length() != 0) {
      ftpClient.changeWorkingDirectory(path);
    }
  }

  // FTP.BINARY_FILE_TYPE | FTP.ASCII_FILE_TYPE
  // Set transform type
  public void setFileType(int fileType) throws IOException {
    ftpClient.setFileType(fileType);
  }

  public void closeServer() {
    try {
      if (ftpClient.isConnected()) {
        ftpClient.disconnect();
        // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
        // BasicMessage.OP_MESSAGE, "Successfully close ftp server:" + server
        // + ":" + port), 1);
        LOG.info("Successfully close ftp server:" + server + ":" + port);
      }
    } catch (IOException e) {
      // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
      // BasicMessage.OP_MESSAGE, "Failed to close ftp server:" + server + ":"
      // + port + "; Cause:" + e.getMessage()), 3);
      LOG.info("Failed to close ftp server:" + server + ":" + port + "; Cause:"
          + e.getMessage());
    }
  }

  public boolean changeDirectory(String path) throws IOException {
    return ftpClient.changeWorkingDirectory(path);
  }

  public boolean createDirectory(String pathName) throws IOException {
    return ftpClient.makeDirectory(pathName);
  }

  public boolean removeDirectory(String path) throws IOException {
    return ftpClient.removeDirectory(path);
  }

  // delete all subDirectory and files.
  public boolean removeDirectory(String path, boolean isAll) throws IOException {

    if (!isAll) {
      return removeDirectory(path);
    }

    FTPFile[] ftpFileArr = ftpClient.listFiles(path);
    if (ftpFileArr == null || ftpFileArr.length == 0) {
      return removeDirectory(path);
    }

    for (FTPFile ftpFile : ftpFileArr) {
      String name = ftpFile.getName();
      if (ftpFile.isDirectory()) {
        // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
        // BasicMessage.OP_MESSAGE, "* [sD]Delete subPath [" + path + "/"
        // + name + "]"), 1);
        LOG.info("* [sD]Delete subPath [" + path + "/" + name + "]");
        removeDirectory(path + "/" + name, true);
      } else if (ftpFile.isFile()) {
        // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
        // BasicMessage.OP_MESSAGE, "* [sF]Delete file [" + path + "/" + name
        // + "]"), 1);
        LOG.info("* [sF]Delete file [" + path + "/" + name + "]");
        deleteFile(path + "/" + name);
      } else if (ftpFile.isSymbolicLink()) {
      } else if (ftpFile.isUnknown()) {
      }
    }
    return ftpClient.removeDirectory(path);
  }

  public boolean existDirectory(String path) throws IOException {
    boolean flag = false;
    FTPFile[] ftpFileArr = ftpClient.listFiles(path);
    for (FTPFile ftpFile : ftpFileArr) {
      if (ftpFile.isDirectory() && ftpFile.getName().equalsIgnoreCase(path)) {
        flag = true;
        break;
      }
    }
    return flag;
  }

  private void list(String path, final String pattern, String excludeStuffix,
      List<String> files, List<String> dirs) throws IOException {
    FTPFile[] ftpFiles = ftpClient.listFiles(path);
    if (ftpFiles == null || ftpFiles.length == 0) {
      return;
    }

    for (FTPFile ftpFile : ftpFiles) {
      if (ftpFile.isFile()) {
        if (!ftpFile.getName().endsWith(excludeStuffix)) {
          if (files != null) {
            files.add(path + "/" + ftpFile.getName());
          }
        }
      } else if (ftpFile.isDirectory()) {
        if (dirs != null) {
          dirs.add(path + "/" + ftpFile.getName());
        }
      }
    }
  }

  private void listFileRecursive(String path, final String pattern,
      String excludeSuffix, List<String> files) throws IOException {
    List<String> dirs = new ArrayList<String>();
    list(path, pattern, excludeSuffix, files, dirs);
    for (String dir : dirs) {
      listFileRecursive(dir, pattern, excludeSuffix, files);
    }
  }

  public List<String> getFileListRecursive(String path, final String pattern,
      String excludeSuffix) throws IOException {
    List<String> files = new ArrayList<String>();
    listFileRecursive(path, pattern, excludeSuffix, files);
    return files;
  }

  public List<String> getFileList(String path, final String pattern,
      String excludeSuffixes) throws IOException {
    // listFiles return contains directory and file, it's FTPFile instance
    // listNames() contains directory, so using following to filer directory.
    // String[] fileNameArr = ftpClient.listNames(path);
    FTPFile[] ftpFiles = ftpClient.listFiles(path);
    // FTPFile[] ftpFiles= ftpClient.listFiles(path, new FTPFileFilter() {
    // @Override
    // public boolean accept(FTPFile file) {
    // return (file.isFile() &&
    // (pattern == null ||
    // file.getName().startsWith(pattern)));
    // }
    // });

    List<String> retList = new ArrayList<String>();
    if (ftpFiles == null || ftpFiles.length == 0) {
      return retList;
    }

    String[] excludeSuffixesArr = excludeSuffixes.split(",");
    for (FTPFile ftpFile : ftpFiles) {
      if (ftpFile.isFile()) {
        boolean excluded = false;
        for (String exclude : excludeSuffixesArr) {
          if (ftpFile.getName().endsWith(exclude))
            excluded = true;
        }
        if (!excluded)
          retList.add(ftpFile.getName());
      }
    }
    return retList;
  }

  public boolean deleteFile(String pathName) throws IOException {
    return ftpClient.deleteFile(pathName);
  }

  public boolean uploadFile(String fileName, String newName) throws IOException {
    boolean flag = false;
    InputStream iStream = null;
    try {
      iStream = new FileInputStream(fileName);
      flag = ftpClient.storeFile(newName, iStream);
    } catch (IOException e) {
      flag = false;
      return flag;
    } finally {
      if (iStream != null) {
        iStream.close();
      }
    }
    return flag;
  }

  public boolean uploadFile(String fileName) throws IOException {
    return uploadFile(fileName, fileName);
  }

  public boolean uploadFile(InputStream iStream, String newName)
      throws IOException {
    boolean flag = false;
    try {
      // can execute [OutputStream storeFileStream(String remote)]
      // Above method return's value is the local file stream.
      flag = ftpClient.storeFile(newName, iStream);
    } catch (IOException e) {
      flag = false;
      return flag;
    } finally {
      if (iStream != null) {
        iStream.close();
      }
    }
    return flag;
  }

  public boolean download(String remoteFileName, String localFileName)
      throws IOException {
    boolean flag = false;
    File outfile = new File(localFileName);
    OutputStream oStream = null;
    try {
      oStream = new FileOutputStream(outfile);
      flag = ftpClient.retrieveFile(remoteFileName, oStream);
    } catch (IOException e) {
      flag = false;
      return flag;
    } finally {
      oStream.close();
    }
    return flag;
  }

  public boolean download(String remoteFileName,
      OutputStream localFileOutputStream) {
    boolean flag = false;
    try {
      flag = ftpClient.retrieveFile(remoteFileName, localFileOutputStream);
    } catch (IOException e) {
      flag = false;
      return flag;
    }
    return flag;
  }

  public InputStream readFile(FtpDirDesc fd) {
    if (fd != null) {
      InputStream is = null;
      String sourceFileName = fd.getFileName();
      try {
        is = ftpClient.retrieveFileStream(sourceFileName);
      } catch (IOException e) {
        // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
        // BasicMessage.OP_MESSAGE,
        // "IOException happened when read file in the ftp data source"), 3);
        LOG.warn("IOException happened when read file in the ftp data source");
        e.printStackTrace();
        is = null;
        if (reconnect(sourceFileName)) {
          // If reconnect successfully, get the input stream again
          try {
            ftpClient.changeWorkingDirectory(fd.getPath());
            is = ftpClient.retrieveFileStream(sourceFileName);
          } catch (IOException e1) {
            // FtpToHdfsMapper.client
            // .send(
            // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
            // "IOException happened when read file again after reconnect"),
            // 3);
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

  private boolean reconnect(String file) {
    boolean flag = false;
    if (file != null) {
      if (!ftpClient.isConnected()) {
        // Reconnect
        if (ftpClient == null) {
          // Generally , this ftpClient shouldn't be null
          ftpClient = new FTPClient();
        }
        for (int i = 0; i < RETRY_TIMES; i++) {
          // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
          // BasicMessage.OP_MESSAGE, " FTP Client" + " reconnect: Retried "
          // + i + " times"), 1);
          LOG.info(" FTP Client" + " reconnect: Retried " + i + " times");
          try {
            Thread.sleep(RETRY_INTERVAL);

            ftpClient.connect(server, port);
            // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
            // BasicMessage.OP_MESSAGE, "Reconnecting to " + server + ":"
            // + port + " ... Retried " + i + " times"), 1);
            LOG.info("Reconnecting to " + server + ":" + port + " ... Retried "
                + i + " times");
            int reply = ftpClient.getReplyCode();
            // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
            // BasicMessage.OP_MESSAGE, String.valueOf(reply)), 1);
            LOG.info("*********" + String.valueOf(reply));
            ftpClient.setDataTimeout(DATA_TIMEOUT);
            // After connection attempt, check the reply code to verify
            // success
            if (!FTPReply.isPositiveCompletion(reply)) {
              ftpClient.disconnect();
              // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
              // BasicMessage.OP_MESSAGE, "FTP server " + server
              // + " refused connection. Retried " + i + " times"), 4);
              LOG.info("FTP server " + server + " refused connection. Retried ");
              continue;
            }
            ftpClient.login(user, password);
            if (ftpClient.isConnected()) {
              flag = true;
              break;
            }
          } catch (InterruptedException e) {
            // FtpToHdfsMapper.client.send(new packet(BasicMessage.NODE,
            // BasicMessage.OP_MESSAGE,
            // "InterruptedException happened when reconnect."), 3);
            LOG.warn("InterruptedException happened when reconnect.");
            e.printStackTrace();
          } catch (IOException e) {
            // FtpToHdfsMapper.client.send(
            // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
            // "IOException happened when reconnect"), 3);
            LOG.warn("IOException happened when reconnect");
            e.printStackTrace();
          }
        }
      }
    }
    return flag;
  }

  public InputStream downFile(String sourceFileName) throws IOException {
    return ftpClient.retrieveFileStream(sourceFileName);
  }

  public boolean moveFile(String srcFileName, String dstFileName)
      throws IOException {
    return ftpClient.rename(srcFileName, dstFileName);
  }

  public boolean completePendingCommand() throws IOException {
    return ftpClient.completePendingCommand();
  }
}
