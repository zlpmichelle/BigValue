package com.cloudera.bigdata.analysis.datagen;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.datagen.GeneratorDriver.ParameterSet;

public class GeneratorTask implements Runnable, Progressable {
  private static final Logger LOG = LoggerFactory
      .getLogger(GeneratorTask.class);

  private ParameterSet parameterSet;
  private List<Long> list;
  private String id;
  private RecordGenerator generator;
  private DateTime dateTime;
  private DateTimeFormatter formatter;
  private FileSystemSink sink;
  private RecordProducer rp = null;
  private long total = 0;
  private long accumulated = 0;
  private long lastDone;
  private boolean isRunning = true;

  public GeneratorTask(Configuration conf, ParameterSet parameterSet,
      List<Long> group, String id) {
    this.parameterSet = parameterSet;
    this.list = group;
    this.id = id;
    generator = new TextRecordGenerator();
    if (generator == null) {
      LOG.error("Cannot initialize the generator "
          + TextRecordGenerator.class.getName());
      System.exit(1);
    }

    LOG.debug("InstanceDoc: " + parameterSet.instanceDoc);
    generator.loadGenerator(parameterSet.instanceDoc);
    dateTime = new DateTime();
    formatter = DateTimeFormat.forPattern("yyyyMMdd-hhmmss");

    conf.set("dfs.replication", "" + parameterSet.replicaNum);
    if (parameterSet.codec != null) {
      conf.set("generator.codec", parameterSet.codec);
    }
    sink = new FileSystemSink(conf);
    sink.setOutputFolder(parameterSet.outputDir);

    computeTotal();
  }

  private void computeTotal() {
    for (int i = 0; i < list.size(); i++) {
      if (parameterSet.useSizeMeasurement) {
        total += list.get(i) * 1024 * 1024;
      } else {
        total += list.get(i);
      }
    }
  }

  private void accumulate(long size) {
    accumulated += size;
  }

  public long getTotal() {
    return total;
  }

  public long getAccumulated() {
    return accumulated / 1024 / 1024;
  }

  public long getLastDone() {
    return lastDone;
  }

  @Override
  public void run() {

    // For streaming generation
    if (parameterSet.neverStop) {
      rp = new RecordProducer();
    }

    int i = 0;
    while (parameterSet.neverStop || i < list.size()) {
      PrintWriter printWriter = null;
      OutputStream outputStream = sink.getOutputStream(getFileName(i));
      if (outputStream == null) {
        throw new NullPointerException("null output stream");
      }
      printWriter = new PrintWriter(new OutputStreamWriter(outputStream));
      long expectedSize = 0;
      if (parameterSet.neverStop) {
        expectedSize = list.get(0);
      } else {
        expectedSize = list.get(i);
      }
      if (parameterSet.useSizeMeasurement) {
        expectedSize = expectedSize * 1024 * 1024;
      }

      long counter = 0;
      long size = 0;

      long recordCounter = 0;
      long beginTime = System.currentTimeMillis();
      long endTime = beginTime;

      while (true) {
        try {
          Record record = generator.generate();
          size += record.getLength();
          if (!parameterSet.useSizeMeasurement) {
            counter++;
          }

          ++recordCounter;
          endTime = System.currentTimeMillis();

          if ((!parameterSet.useSizeMeasurement && counter >= expectedSize)
              || (parameterSet.useSizeMeasurement && size >= expectedSize)

              || (parameterSet.neverStop && (endTime - beginTime >= 10000))) {
            printWriter.close();

            if (rp != null) {
              rp.put(parameterSet.outputDir + "/" + getFileName(i),
                  new Integer(i).toString());
            }

            LOG.info("############ the time that generate the record: "
                + (endTime - beginTime));
            LOG.info("############ the number of the code is:" + recordCounter);

            LOG.info("Finish writing "
                + getFileName(i)
                + " with "
                + ((parameterSet.useSizeMeasurement) ? (size + " bytes")
                    : (counter + " records")));
            break;
          } else {
            printWriter.println(record.getAsString());
          }
        } catch (Exception e) {
          LOG.error("", e);
        }
      }
      accumulate(size);
      lastDone = counter;

      i++;
    }

    isRunning = false;
  }

  public String getName() {
    return "Thread-" + id;
  }

  public String getFileName(int fileId) {
    return getName() + "-" + "File-" + fileId;
  }

  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public double getProgress() {
    // TODO Auto-generated method stub
    return 0;
  }

}
