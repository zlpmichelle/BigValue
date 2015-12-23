package com.cloudera.bigdata.analysis.datagen;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemSink implements GeneratorSink {
  private final static Logger LOG = LoggerFactory.getLogger(FileSystemSink.class);
  private CompressionCodec codec;
  private Configuration conf;
  private FileSystem fileSystem;
  private Path outputPath; 
  
  public FileSystemSink(Configuration configuration){
    this.conf = configuration;
    // disable CRC generation for LocalFileSystem
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
    String codecName = conf.get("generator.codec");
    if(null!=codecName){
      CompressionCodecFactory codecFactory = 
          new CompressionCodecFactory(conf);
      codec = codecFactory.getCodecByName(codecName);
      if(codec==null){
        LOG.error("Unrecognized codec name: " + conf.get("generator.compression"));
        System.exit(1);
      }
    }
  }

  @Override
  public void sink(Record record) {
    
  }

  @Override
  public void setOutputFolder(String outputDir) {
    outputPath = new Path(outputDir);
    try {
      fileSystem = outputPath.getFileSystem(conf);
      // configuration returned by fileSystem.getConf() 
      // will not be the same as the above conf; need to check why?
      outputPath.makeQualified(fileSystem);
    } catch (IOException e) {
      LOG.error("Cannot resolve the correct file system from: " + outputPath, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public OutputStream getOutputStream(String fileName) {
    try{
      OutputStream outputStream = 
          fileSystem.create(new Path(outputPath, fileName), 
              (short)conf.getInt("dfs.replication", GeneratorDriver.DEFAULT_REPLICA_NUM));
      FsPermission permission = new FsPermission("777");
      fileSystem.setPermission(new Path(outputPath, fileName), permission);
      if(null!=codec){
        outputStream = codec.createOutputStream(outputStream);
      }
      return outputStream;
    }catch (IOException e) {
      LOG.error("Cannot create the output stream for: " + outputPath, e);
      Thread.currentThread().interrupt();
      return null;
    }
  }

}
