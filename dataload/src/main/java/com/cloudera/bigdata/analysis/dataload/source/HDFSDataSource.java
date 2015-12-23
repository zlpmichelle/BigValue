package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.io.HDFSFileObject;

public class HDFSDataSource extends DataSource {
  public static Log LOG = LogFactory.getLog(HDFSDataSource.class);
  static {
    DataSource.define(HDFSDataSource.class, new HDFSDataSource());
  }

  private FileSystem fs = null;
  private Configuration conf;
  private Path[] hdfsDirDescArray;
  private String serverAddr;

  private HDFSDataSource() {
  }

  @Override
  public void init(Configuration configuration) throws IOException {
    this.conf = configuration;
  }

  @Override
  public void connect() throws IOException {
    serverAddr = conf.get(Constants.FS_DEFAULT_NAME_KEY);
    fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      LOG.error("Cannot connect to a DistributedFileSystem");
      System.exit(1);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public List<FileObject> getFileList() throws IOException {
    String hdfsDirs = conf.get(Constants.HDFSDIRS);
    if (StringUtils.isEmpty(hdfsDirs)) {
      LOG.error("Please set the hdfsDirs in the properties fiels.");
      System.exit(1);
    }

    String[] hdfsDirArray = hdfsDirs.split(Constants.DIRS_SEPARATOR);
    this.hdfsDirDescArray = new Path[hdfsDirArray.length];
    for (int i = 0; i < hdfsDirArray.length; i++) {
      Path hdfsDirDesc = new Path(hdfsDirArray[i]);
      hdfsDirDescArray[i] = hdfsDirDesc;
      LOG.info("Add new hdfs dir desc : " + hdfsDirDesc.toString());
    }
    List<FileObject> retList = new ArrayList<FileObject>();
    for (int i = 0; i < hdfsDirDescArray.length; i++) {
      retList.addAll(getFileList(hdfsDirDescArray[i]));
    }
    return retList;
  }

  private List<FileObject> getFileList(Path hdfsDirDesc) throws IOException {
    List<FileObject> retList = new ArrayList<FileObject>();
    if (fs.exists(hdfsDirDesc)) {
      if (fs.isFile(hdfsDirDesc)) {
        retList.add(new HDFSFileObject(serverAddr, hdfsDirDesc.getParent()
            .toString(), hdfsDirDesc.getName()));
      } else {
        FileStatus files[] = fs.listStatus(hdfsDirDesc);
        if (files == null || files.length == 0) {
          return retList;
        }
        for (FileStatus file : files) {
          if (!file.isDir()) {
            Path path = file.getPath();
            retList.add(new HDFSFileObject(serverAddr, path.getParent()
                .toString(), path.getName()));
          } else {
            retList.addAll(getFileList(file.getPath()));
          }
        }
      }
    }
    return retList;
  }

  @Override
  public InputStream readFile(FileObject file) {
    Path path = new Path(file.getCanonicalPath());

    InputStream is = null;
    try {
      is = fs.open(path);
    } catch (IOException e) {
      LOG.error("Failed to open file " + path.toString(), e);
    }

    return is;

  }

  @Override
  public void closeFile(FileObject file, InputStream is,
      FileHandleStatus fileHandleStatus, Exception e) throws IOException {

    if (is != null) {
      is.close();
    }
    /*
     * Path temp = new Path(file.getPath() + "/tmp/"); if (!fs.exists(temp)) {
     * fs.mkdirs(temp); } fs.rename(new Path(file.getCanonicalPath()), new
     * Path(file.getPath() + "/tmp/" + file.getName()));
     */
  }

  @Override
  public Class<? extends FileObject> getFileObjectClass() {
    return HDFSFileObject.class;
  }

}
