package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.ftp2hdfs2hbase.ZJCMCCConstants;
import com.cloudera.bigdata.analysis.dataload.source.FtpDirDesc;
import com.cloudera.bigdata.analysis.dataload.util.FtpUtil;

public class Ftp2HdfsMapper<K> extends
    Mapper<K, Text, NullWritable, NullWritable> {

  static final Logger LOG = LoggerFactory.getLogger(Ftp2HdfsMapper.class);
  static final int FLAG_BOUND = 1000;

  // private FTPUtil ftpUtil = null;
  private FileSystem fs;
  private Path outputDir;
  private FtpUtil ftpUtil = new FtpUtil();

  private boolean deletFiles;
  private boolean moveFiles;

  // public static DisLogObj client = null;

  @Override
  public void setup(Context context) {
    // start client in mapper
    // DisLog log = new DisLog();
    // String ServerIp = Util.getServerIPorHostname();
    // client = log.StartClient(ServerIp, 4399, LOG, 0);
    // client.send(new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
    // "Setup FTP to HDFS Dataload Mapreduce"), 1);
    LOG.info("Setup FTP to HDFS Dataload Mapreduce");

    ftpUtil = new FtpUtil();
    Configuration conf = context.getConfiguration();

    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      // client
      // .send(
      // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE, e
      // .getMessage()), 4);
      LOG.warn(e.getMessage());
      e.printStackTrace();
    }

    String output = conf.get("mapper.fileOutputPath");
    outputDir = new Path(output);

    deletFiles = conf.getBoolean("mapper.deleteFiles", false);
    moveFiles = conf.getBoolean("mapper.moveFiles", false);
    // client.send(new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
    // "The output path is to " + outputDir.toString()), 1);
    LOG.info("The output path is to " + outputDir.toString());
  }

  @Override
  public void map(K key, Text value, Context context) {
    try {
      String line = value.toString();
      LOG.info("Read input line: " + line);

      // Get new file path
      FtpDirDesc fd = new FtpDirDesc(line, true);
      // LOG.info("fd: " + fd.toString());

      // Connect the server.

      ftpUtil.connectServer(fd.getServer(), fd.getPort(), fd.getUser(),
          fd.getPassword(), "");

      // If the file in a different server or uid or passwd, reconnect.
      // if (previousFd == null) {
      // LOG.info("Connecting ftp server: " + fd.getFtpServer() + ":"
      // + fd.getFtpPort() + ":" + fd.getFtpUser() + ":"
      // + fd.getFtpPasswd());
      // ftpUtil.connectServer(fd.getFtpServer(), fd.getFtpPort(),
      // fd.getFtpUser(), fd.getFtpPasswd(), "");
      // }
      // else if (!previousFd.equals(fd)) {
      // LOG.info("Connecting ftp server: " + fd.getFtpServer() + ":"
      // + fd.getFtpPort() + ":" + fd.getFtpUser() + ":"
      // + fd.getFtpPasswd());
      //
      // ftpUtil.closeServer();
      //
      // }

      // Get the target file.
      ftpUtil.setFileType(FtpUtil.ASCII_FILE_TYPE);
      ftpUtil.changeDirectory(fd.getPath());

      // Get the output path of the file.
      Path outputFile = new Path(outputDir, fd.getFileName());

      LOG.info("Write to file " + outputFile.toString());

      FSDataOutputStream out = null;
      // if(fs.exists(outputFile) && fs.isFile(outputFile)) {
      // out = fs.append(outputFile);
      // }
      // else if (!fs.exists(outputFile)) {
      // out = fs.create(outputFile);
      // }

      // Try to download the file.
      if (!fs.exists(outputFile)) {
        out = fs.create(outputFile);
      } else {
        // client.send(new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
        // "Failed to write to hdfs " + outputFile), 4);
        LOG.warn("Failed to write to hdfs " + outputFile);
        ftpUtil.closeServer();
        return;
      }

      LOG.info("Fetching ftp file: " + fd.toString());
      InputStream fileDown = ftpUtil.readFile(fd);

      // Copy data from file in ftp.
      long copySize = -1;
      if (fileDown != null) {
        copySize = IOUtils.copyLarge(fileDown, out);
        moveFiles = true;
      } else {
        moveFiles = false;
      }
      LOG.info(copySize + " bytes copied from " + fd.toString() + "to"
          + outputFile);

      if (out != null) {
        out.flush();
        out.close();
        context.getCounter(ZJCMCCConstants.COUNTER.FILECOUNT).increment(1);
      }
      if (fileDown != null) {
        fileDown.close();
      }

      if (!ftpUtil.completePendingCommand() || copySize == -1) {
        // client.send(new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
        // "Error copying remote file(return null): " + fd.toString()), 4);
        LOG.error("Error copying remote file(return null): " + fd.toString());
        ftpUtil.closeServer();
        // throw new
        // IOException("Error opening remote file(return null): "+fd.toString());
        return;
      }

      LOG.info("Successeded to fetch ftp file: " + fd.toString());

      // Delete the completed files.

      if (moveFiles) {
        if (ftpUtil.moveFile(fd.getPath() + "/" + fd.getFileName(),
            fd.getPath() + "2/" + fd.getFileName())) {
          // LOG.info("Successfully move remote file " +
          // fd.getFtpDir()+"/"+fd.getFileName() + ".");
        } else {
          LOG.info("Failed to move remote file " + fd.getPath() + "/"
          + fd.getFileName() + ".");
        }
      }

      if (deletFiles) {
        if (ftpUtil.deleteFile(fd.getPath() + "/" + fd.getFileName())) {
          // LOG.info("Successfully deleted remote file " +
          // fd.getFtpDir()+"/"+fd.getFileName() + ".");
        } else {
          // client.send(
          // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE,
          // "Failed to delete remote file " + fd.getPath() + "/"
          // + fd.getFileName() + "."), 4);
          LOG.info("Failed to delete remote file " + fd.getPath() + "/"
              + fd.getFileName() + ".");
        }
      }
      ftpUtil.closeServer();

    } catch (SocketException e) {
      e.printStackTrace();
      // client
      // .send(
      // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE, e
      // .getMessage()), 1);
      LOG.warn(e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
      // client
      // .send(
      // new packet(BasicMessage.NODE, BasicMessage.OP_MESSAGE, e
      // .getMessage()), 1);
      LOG.warn(e.getMessage());
    }
  }

  @Override
  public void cleanup(Context context) {
    // if(fs != null) {
    // try {
    // fs.close();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // LOG.error("Failed to close file system.", e);
    // }
    // }

  }
}
