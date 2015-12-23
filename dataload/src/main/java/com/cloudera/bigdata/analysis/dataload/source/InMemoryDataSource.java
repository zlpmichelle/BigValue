package com.cloudera.bigdata.analysis.dataload.source;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;

public class InMemoryDataSource extends DataSource {

  static {
    DataSource.define(InMemoryDataSource.class, new InMemoryDataSource());
  }

  private Configuration conf;

  private InMemoryDataSource() {

  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public List<FileObject> getFileList() throws IOException {
    int fileNum = conf.getInt(Constants.FILE_NUM_KEY,
        Constants.DEFAULT_FILE_NUM);

    List<FileObject> fileList = new ArrayList<FileObject>(fileNum);
    for (int i = 0; i < fileNum; i++) {
      fileList.add(new InMemFileObject(Constants.IN_MEM_FILENAME_PREFIX + i,
          conf.getLong(Constants.RECORD_NUM_PER_FILE_KEY,
              Constants.DEFAULT_RECORD_NUM_PER_FILE)));
    }

    return fileList;
  }

  @Override
  public InputStream readFile(FileObject file) throws IOException {
    return null;
  }

  @Override
  public void closeFile(FileObject file, InputStream is,
      FileHandleStatus fileHandleStatus, Exception e) throws IOException {

  }

  @Override
  public Class<? extends FileObject> getFileObjectClass() {
    return InMemFileObject.class;
  }

  public static class InMemFileObject implements FileObject {

    private String fileName;
    private Long size;

    public InMemFileObject() {

    }

    public InMemFileObject(String name, long size) {
      this.fileName = name;
      this.size = new Long(size);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      fileName = dataInput.readUTF();
      size = dataInput.readLong();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeUTF(fileName);
      dataOutput.writeLong(size);
    }

    @Override
    public int compareTo(FileObject obj) {
      // TODO Auto-generated method stub
      if (!(obj instanceof InMemFileObject)) {
        return -1;
      }

      InMemFileObject thatObj = (InMemFileObject) obj;
      return fileName.compareTo(thatObj.fileName) != 0 ? fileName
          .compareTo(thatObj.fileName) : size.compareTo(thatObj.size);
    }

    @Override
    public String toString() {
      return fileName + "(RecordNum:" + size + ")";
    }

    @Override
    public String getName() {
      return fileName;
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public String getCanonicalPath() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getHostname() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getPath() {
      // TODO Auto-generated method stub
      return null;
    }

  }

}
