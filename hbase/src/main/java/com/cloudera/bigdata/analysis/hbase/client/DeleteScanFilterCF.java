package com.cloudera.bigdata.analysis.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteScanFilterCF {

  private static HTable table;

  public static void main(String[] args) throws IOException {
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, args[0]);

      List<Filter> filters = new ArrayList<Filter>();

      Filter famFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(Bytes.toBytes("f2")));
      filters.add(famFilter);

      Filter colFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(Bytes.toBytes("ORG_CFEE")));
      filters.add(colFilter);

      if (args[1].equals("smaller")) {
        Filter valFilter = new ValueFilter(CompareFilter.CompareOp.LESS,
            new BinaryComparator(Bytes.toBytes(args[2])));
        System.out.println("scan samller than " + args[2]);
        filters.add(valFilter);
      } else {
        Filter valFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(Bytes.toBytes(args[2])));
        System.out.println("scan equal to " + args[2]);
        filters.add(valFilter);
      }

      FilterList filt = new FilterList(FilterList.Operator.MUST_PASS_ALL,
          filters);

      Scan scan = new Scan();
      scan.setFilter(filt);
      ResultScanner scanner = table.getScanner(scan);
      long count = 0;

      // print
      System.out.println("Scanning table... ");
      List<Delete> listDeleteRow = new ArrayList<Delete>();
      for (Result result : scanner) {
        count++;
        // System.out.println("getRow:" + Bytes.toString(result.getRow()));
        for (KeyValue kv : result.raw()) {
          // System.out.println("Family - " + Bytes.toString(kv.getFamily()));
          // System.out
          // .println("Qualifier - " + Bytes.toString(kv.getQualifier()));
          // System.out.println("kv:" + kv + ", Key: "
          // + Bytes.toString(kv.getRow()) + ", Value: "
          // + Bytes.toString(kv.getValue()));
          // delete
          Delete d = new Delete(result.getRow());
          listDeleteRow.add(d);
        }
      }
      System.out.println("will delete " + count + " rows... ");
      // delete table
      table.delete(listDeleteRow);
      System.out.println("Finished scan and delete table.");
    } catch (Exception e) {
      // TODO: handle exception
    }
  }
}
