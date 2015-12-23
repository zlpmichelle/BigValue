package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/**
 * DataSource handles connection to the source of input data.
 * It provides API to let data load client get the whole list of input data.
 * It's also responsible for open stream and close stream for input data.
 * 
 * Framework will help wire DataSource and FileParser.
 */
public abstract class DataSource {
  public static enum FileHandleStatus {
    HANDLED_SUCCESSFUL, INTERRUPTED, IO_ERROR, PARSE_ERROR, HBASE_ERROR
  }

  private static HashMap<Class<? extends DataSource>, DataSource> dataSources;

  static {
    dataSources = new HashMap<Class<? extends DataSource>, DataSource>();
  }

  public static DataSource getDataSource(Class<? extends DataSource> clazz) {
    DataSource source = dataSources.get(clazz);
    if (source == null) {
      // Try to force initialize source implementation
      Util.forceInit(clazz.getName());
    }
    return dataSources.get(clazz);
  }

  public static DataSource getDataSource(String className) {
    Class<?> clazz = Util.forceInit(className);
    Class<? extends DataSource> subClazz;
    try {
      subClazz = clazz.asSubclass(DataSource.class);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Cannot cast ", e);
    }

    return getDataSource(subClazz);
  }

  public static void define(Class<? extends DataSource> clazz,
      DataSource dataSource) {
    dataSources.put(clazz, dataSource);
  }

  /** Initialize this data source */
  public abstract void init(Configuration conf) throws IOException;

  /** Connect this data source */
  public abstract void connect() throws IOException;

  /** Close this data source after fetch all the files */
  public abstract void close() throws IOException;

  /** Get the file list from this data source */
  public abstract List<FileObject> getFileList() throws IOException;

  /**
   * Read file by this data source
   * 
   * @return InputStream
   * */
  public abstract InputStream readFile(FileObject file) throws IOException;

  /** Close the file when the file has been fetched completely */
  public abstract void closeFile(FileObject file, InputStream is,
      FileHandleStatus fileHandleStatus, Exception e) throws IOException;

  /** Return the FileObject's Class, mainly used in the mapreduce mode */
  public abstract Class<? extends FileObject> getFileObjectClass();

}
