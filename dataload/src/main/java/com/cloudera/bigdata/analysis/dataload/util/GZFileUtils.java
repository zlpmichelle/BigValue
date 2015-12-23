package com.cloudera.bigdata.analysis.dataload.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class GZFileUtils {

  private static final int EMPTY_FILE_GZ_SIZE = 64;

  private static Logger logger = Logger.getLogger(GZFileUtils.class);

  public static int BUFFER_SIZE = 512 * 1024;

  public static int GZIP_COMPRESSION_RATIO = 10;

  public static void splitFileToHdfs(String inputGZDir, String hdfsDir,
      long hdfsGZSize, int workerThreads) throws IOException,
      InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(workerThreads);
    final Queue<File> fileQueue = new ConcurrentLinkedQueue<File>();
    loadWorkItems(inputGZDir, fileQueue);
    final FileSystem fs = FileSystem.get(new Configuration());
    for (int n = 1; n <= workerThreads; n++) {
      new Thread(new GZSplitter(fs, fileQueue, hdfsDir, hdfsGZSize,
          countDownLatch)).start();
    }

    countDownLatch.await();
  }

  private static void loadWorkItems(String inputGZDir, Queue<File> queue)
      throws IOException {

    File dir = new File(inputGZDir);
    File[] gzFilesNotEmpty = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {

        if (pathname.isDirectory())
          return false;

        String fileName = pathname.getName();
        if (fileName.endsWith(".gz")) {
          long size = pathname.length();

          if (size > EMPTY_FILE_GZ_SIZE)
            return true;
        }

        return false;

      }
    });

    for (File file : gzFilesNotEmpty) {
      queue.add(file);
    }
  }

  private static class GZSplitter implements Runnable {

    private final FileSystem fs;
    private final Queue<File> queue;
    private final String outputBaseDir;
    private final long targetSize;
    private final CountDownLatch latch;

    byte[] buffer;

    private GZSplitter(FileSystem fs, Queue<File> queue, String outputBaseDir,
        long targetSize, CountDownLatch latch) {
      this.fs = fs;
      this.queue = queue;
      this.outputBaseDir = outputBaseDir;
      this.targetSize = targetSize;
      this.latch = latch;

      buffer = new byte[BUFFER_SIZE];
    }

    @Override
    public void run() {
      try {
        File file = null;
        while ((file = queue.poll()) != null) {
          InputStream is = new FileInputStream(file);

          logger.info("****** file " + file.toString());

          GZIPInputStream gzi = new GZIPInputStream(is);

          int split = -1;

          String outputFile = getOutputDirForGZFile(file, outputBaseDir,
              ++split);

          OutputStream os = fs.create(new Path(outputFile));
          GZIPOutputStream gzo = new GZIPOutputStream(os);

          int outputSize = 0;

          // logger.info("****** outputfile:" + outputFile.toString());

          int readSize = 0;
          while ((readSize = gzi.read(buffer)) != -1) {
            if (outputSize + readSize >= targetSize * GZIP_COMPRESSION_RATIO) {
              gzo.write(buffer, 0, readSize);
              outputSize += readSize;

              int c;
              while ((c = gzi.read()) != -1) {
                gzo.write(c);
                outputSize++;
                if (c == '\n')
                  break;
              }
              // bug fix end
              gzo.flush();
              gzo.finish();
              os.close();
              gzo.close();

              outputFile = getOutputDirForGZFile(file, outputBaseDir, ++split);
              os = fs.create(new Path(outputFile));
              gzo = new GZIPOutputStream(os);
              outputSize = 0;
            } else {
              gzo.write(buffer, 0, readSize);
              outputSize += readSize;

              // logger.info("******outputSize " + outputSize);
            }
          }

          gzo.flush();
          gzo.finish();
          os.close();
          gzo.close();

          is.close();
          gzi.close();

          logger.info("Finish splitting " + file + " to hdfs... "
              + outputBaseDir);
        }
        latch.countDown();

      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    private String getOutputDirForGZFile(File file, String hdfsOutputBaseDir,
        int split) {

      StringBuilder sb = new StringBuilder();
      sb.append(hdfsOutputBaseDir);

      sb.append(File.separator);
      String fileName = file.getName();
      String fileNameWithoutExtension = fileName.substring(0,
          fileName.length() - 3);
      sb.append(fileNameWithoutExtension);
      // sb.append(File.separator);

      if (split < 10) {
        sb.append("0000");
      } else if (split < 100) {
        sb.append("000");
      } else if (split < 1000) {
        sb.append("00");
      } else if (split < 10000) {
        sb.append("0");
      }

      sb.append(split + ".gz");
      logger.info("===" + sb.toString());
      return sb.toString();

    }
  }

  private static final String USAGE_STR = "com.cloudera.bigdata.analysis.dataload.util.GZFileUtils <inputGZDir> <hdfsDir> <hdfsGZSize_B> <workerThreads>";

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println(USAGE_STR);
    }

    String inputGZDir = args[0];
    String hdfsDir = args[1];
    long hdfsGZSize = Long.parseLong(args[2]);
    int workerThreads = Integer.parseInt(args[3]);

    splitFileToHdfs(inputGZDir, hdfsDir, hdfsGZSize, workerThreads);

  }
}
