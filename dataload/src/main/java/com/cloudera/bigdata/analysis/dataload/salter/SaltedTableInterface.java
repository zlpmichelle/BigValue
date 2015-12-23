package com.cloudera.bigdata.analysis.dataload.salter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
//import org.apache.hadoop.hbase.client.MergeSortScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * This operator is used to access(get, delete, put, scan) salted table easily.
 *
 */
public class SaltedTableInterface implements HTableInterface {

  private KeySalter salter;
  private HTableInterface table;

  public SaltedTableInterface(KeySalter salter) {
    this.salter = salter;
  }

  public SaltedTableInterface(HTableInterface table, KeySalter salter) {
    this.table = table;
    this.salter = salter;
  }

  @Override
  public Result get(Get get) throws IOException {
    return unSalt(table.get(salt(get)));
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return getScanner(scan, null);
  }

  /**
   * Allow to scan on specified salts.
   * 
   * @param scan
   * @param salts
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts) throws IOException {
    // return new SaltedScanner(scan, salts, false);
    return null;
  }

  /**
   * Allow to scan on specified salts.
   * 
   * @param scan
   * @param salts
   * @param keepSalt
   *          , whether to keep the salt in the key.
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts, boolean keepSalt)
      throws IOException {
    // return new SaltedScanner(scan, salts, keepSalt);
    return null;
  }

  @Override
  public void put(Put put) throws IOException {
    table.put(salt(put));
  }

  @Override
  public void delete(Delete delete) throws IOException {
    table.delete(salt(delete));
  }

  private Get salt(Get get) throws IOException {
    if (null == get) {
      return null;
    }
    Get newGet = new Get(salter.salt(get.getRow()));
    newGet.setFilter(get.getFilter());
    newGet.setCacheBlocks(get.getCacheBlocks());
    newGet.setMaxVersions(get.getMaxVersions());
    newGet.setTimeRange(get.getTimeRange().getMin(), get.getTimeRange()
        .getMax());
    newGet.getFamilyMap().putAll(get.getFamilyMap());
    return newGet;
  }

  private Delete salt(Delete delete) {
    if (null == delete) {
      return null;
    }
    byte[] newRow = salter.salt(delete.getRow());
    Delete newDelete = new Delete(newRow);

    Map<byte[], List<Cell>> newMap = salt(delete.getFamilyCellMap());
    newDelete.getFamilyCellMap().putAll(newMap);
    return newDelete;
  }

  public Put salt(Put put) {
    if (null == put) {
      return null;
    }
    byte[] newRow = salter.salt(put.getRow());
    Put newPut = new Put(newRow);
    Map<byte[], List<Cell>> newMap = salt(put.getFamilyCellMap());
    newPut.getFamilyCellMap().putAll(newMap);
    newPut.setWriteToWAL(put.getWriteToWAL());
    return newPut;
  }

  private Map<byte[], List<Cell>> salt(Map<byte[], List<Cell>> familyMap) {
    if (null == familyMap) {
      return null;
    }
    Map<byte[], List<Cell>> result = new HashMap<byte[], List<Cell>>();
    for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
      List<Cell> kvs = entry.getValue();
      if (null != kvs) {
        List<Cell> newKvs = new ArrayList<Cell>();
        for (int i = 0; i < kvs.size(); i++) {
          newKvs.add(salt(kvs.get(i)));
        }
        result.put(entry.getKey(), newKvs);
      }
    }
    return result;
  }

  private Cell salt(Cell kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRow = salter.salt(kv.getRowArray());
    return new KeyValue(newRow, 0, newRow.length, kv.getFamilyArray(),
        kv.getFamilyOffset(), kv.getFamilyLength(), kv.getFamilyArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp(),
        KeyValue.Type.codeToType(kv.getTypeByte()), kv.getFamilyArray(),
        kv.getValueOffset(), kv.getValueLength());
  }

  private Result unSalt(Result result) {
    if (null == result) {
      return null;
    }
    Cell[] results = result.rawCells();
    if (null == results) {
      return null;
    }
    Cell[] newResults = new Cell[results.length];

    for (int i = 0; i < results.length; i++) {
      newResults[i] = unSalt(results[i]);
    }
    return Result.create(newResults);
  }

  private Cell unSalt(Cell kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRowKey = salter.unSalt(kv.getRowArray());
    return new KeyValue(newRowKey, 0, newRowKey.length, kv.getFamilyArray(),
        kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp(),
        KeyValue.Type.codeToType(kv.getTypeByte()), kv.getValueArray(),
        kv.getValueOffset(), kv.getValueLength());
  }

  public HTableInterface getRawTable() {
    return this.table;
  }

  @Override
  public byte[] getTableName() {
    return table.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  @Override
  public boolean isAutoFlush() {
    return table.isAutoFlush();
  }

  @Override
  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    Get newGet = salt(get);
    return table.exists(newGet);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (null == gets || gets.size() == 0) {
      return null;
    }
    Result[] result = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      Get newGet = salt(gets.get(i));
      result[i] = unSalt(table.get(newGet));
    }
    return result;
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    if (null == puts || puts.size() == 0) {
      return;
    }
    List<Put> newPuts = new ArrayList<Put>(puts.size());
    for (int i = 0; i < puts.size(); i++) {
      newPuts.add(salt(puts.get(i)));
    }
    table.put(newPuts);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    if (null == deletes || deletes.size() == 0) {
      return;
    }
    List<Delete> newDeletes = new ArrayList<Delete>(deletes.size());
    for (int i = 0; i < deletes.size(); i++) {
      newDeletes.add(salt(deletes.get(i)));
    }
    table.delete(newDeletes);
  }

  @Override
  public Result append(Append append) throws IOException {
    Result result = table.append(salt(append));
    return unSalt(result);
  }

  private Append salt(Append append) {
    if (null == append) {
      return null;
    }
    byte[] newRow = salter.salt(append.getRow());
    Append newAppend = new Append(newRow);

    Map<byte[], List<Cell>> newMap = salt(append.getFamilyCellMap());
    newAppend.getFamilyCellMap().putAll(newMap);
    return newAppend;
  }

  // @Override
  // public RowLock lockRow(byte[] row) throws IOException {
  // byte[] newRow = salter.salt(row);
  // return table.lockRow(newRow);
  // }
  //
  // @Override
  // public void unlockRow(RowLock rl) throws IOException {
  // table.unlockRow(rl);
  // }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    byte[] newRow = salter.salt(row);
    Result result = table.getRowOrBefore(newRow, family);
    return unSalt(result);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, value, newPut);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, value, newDelete);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount,
        writeToWAL);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  // @Override
  // public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T>
  // protocol,
  // byte[] row) {
  // throw new
  // UnsupportedOperationException("Please use getRawTable to get underlying table");
  // }
  //
  // @Override
  // public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
  // Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable)
  // throws IOException, Throwable {
  // throw new
  // UnsupportedOperationException("Please use getRawTable to get underlying table");
  // }
  //
  // @Override
  // public <T extends CoprocessorProtocol, R> void coprocessorExec(
  // Class<T> protocol, byte[] startKey, byte[] endKey, Call<T, R> callable,
  // Callback<R> callback) throws IOException, Throwable {
  // throw new
  // UnsupportedOperationException("Please use getRawTable to get underlying table");
  // }

  /**
   * This scanner will merge sort the scan result, and remove the salts
   * 
   */
  // private class SaltedScanner implements ResultScanner {
  //
  // private MergeSortScanner scanner;
  // private boolean keepSalt;
  //
  // public SaltedScanner (Scan scan, byte[][] salts, boolean keepSalt) throws
  // IOException {
  // Scan[] scans = salt(scan, salts);
  // this.keepSalt = keepSalt;
  // this.scanner = new MergeSortScanner(scans, table,
  // salter.getSaltLength());
  // }
  //
  // @Override
  // public Iterator<Result> iterator() {
  // return new Iterator<Result>() {
  //
  // public boolean hasNext() {
  // return scanner.iterator().hasNext();
  // }
  //
  // public Result next() {
  // if (keepSalt) {
  // return scanner.iterator().next();
  // }
  // else {
  // return unSalt(scanner.iterator().next());
  // }
  // }
  //
  // public void remove() {
  // throw new UnsupportedOperationException();
  // }
  // };
  // }
  //
  // @Override
  // public Result next() throws IOException {
  // if (keepSalt) {
  // return scanner.next();
  // }
  // else {
  // return unSalt(scanner.next());
  // }
  //
  // }
  // @Override
  // public Result[] next(int nbRows) throws IOException {
  // ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
  // for(int i = 0; i < nbRows; i++) {
  // Result next = next();
  // if (next != null) {
  // resultSets.add(next);
  // } else {
  // break;
  // }
  // }
  // return resultSets.toArray(new Result[resultSets.size()]);
  // }
  //
  // @Override
  // public void close() {
  // scanner.close();
  // }
  //
  // private Scan[] salt(Scan scan, byte[][] salts) throws IOException {
  //
  // byte[][] splits = null;
  // if (null != salts) {
  // splits = salts;
  // }
  // else {
  // splits = salter.getAllSalts();
  // }
  // Scan[] scans = new Scan[splits.length];
  // byte[] start = scan.getStartRow();
  // byte[] end = scan.getStopRow();
  //
  // for (int i = 0; i < splits.length; i++) {
  // scans[i] = new Scan(scan);
  // scans[i].setStartRow(concat(splits[i], start));
  // if (end.length == 0) {
  // scans[i].setStopRow( (i == splits.length - 1) ?
  // HConstants.EMPTY_END_ROW : splits[i + 1]);
  // }
  // else {
  // scans[i].setStopRow(concat(splits[i], end));
  // }
  // }
  // return scans;
  // }
  //
  // private byte[] concat(byte[] prefix, byte[] row) {
  // if (null == prefix || prefix.length == 0) {
  // return row;
  // }
  // if (null == row || row.length == 0) {
  // return prefix;
  // }
  // byte[] newRow = new byte[row.length + prefix.length];
  // if (row.length != 0) {
  // System.arraycopy(row, 0, newRow, prefix.length, row.length);
  // }
  // if (prefix.length != 0) {
  // System.arraycopy(prefix, 0, newRow, 0, prefix.length);
  // }
  // return newRow;
  // }
  // }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    table.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  @Override
  public long getWriteBufferSize() {
    return table.getWriteBufferSize();
  }

  @Override
  public void setAutoFlush(boolean arg0) {
    table.setAutoFlush(arg0);
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> arg0, Callback<R> arg1)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public <R> void batchCallback(List<? extends Row> arg0, Object[] arg1,
      Callback<R> arg2) throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  // @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor arg0, Message arg1, byte[] arg2, byte[] arg3, R arg4)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  // @Override
  public <R extends Message> void batchCoprocessorService(
      MethodDescriptor arg0, Message arg1, byte[] arg2, byte[] arg3, R arg4,
      Callback<R> arg5) throws ServiceException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] arg0) {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> arg0, byte[] arg1, byte[] arg2, Call<T, R> arg3)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> arg0,
      byte[] arg1, byte[] arg2, Call<T, R> arg3, Callback<R> arg4)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException(
        "Please use getRawTable to get underlying table");
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    // TODO Auto-generated method stub
    Boolean[] ret = new Boolean[gets.size()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = table.exists(salt(gets.get(i)));
    }
    return ret;
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, Durability durability) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount,
        durability);
  }

  @Override
  public void setAutoFlushTo(boolean arg0) {
    table.setAutoFlushTo(arg0);
  }

  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations mutation)
      throws IOException {
// TODO Auto-generated method stub
    return false;
  }

}
