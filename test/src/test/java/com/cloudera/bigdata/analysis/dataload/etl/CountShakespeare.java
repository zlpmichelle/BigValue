package com.cloudera.bigdata.analysis.dataload.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class CountShakespeare {
  public static class Map extends TableMapper<Text, LongWritable> {
    public static enum Counters {
      ROWS, SHAKESPEAREAN
    };

    private boolean containsShakespeare(String msg) {
      return false;
      // ...
    }

    @Override
    protected void map(ImmutableBytesWritable rowkey, Result result,
        Context context) {
      byte[] b = result.getColumnLatest(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL)
          .getValue();
      String msg = Bytes.toString(b);
      if (msg != null && !msg.isEmpty())
        context.getCounter(Counters.ROWS).increment(1);
      if (containsShakespeare(msg))
        context.getCounter(Counters.SHAKESPEAREAN).increment(1);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "TwitBase Shakespeare counter");
    job.setJarByClass(CountShakespeare.class);

    Scan scan = new Scan();
    scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);

    TableMapReduceUtil.initTableMapperJob(Bytes.toString(TwitsDAO.TABLE_NAME),
        scan, Map.class, ImmutableBytesWritable.class, Result.class, job);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}