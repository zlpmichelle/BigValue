package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.JAXBElement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.FileInfo;
import com.cloudera.bigdata.analysis.dataload.HBaseUpdater;
import com.cloudera.bigdata.analysis.dataload.RecordWrapper;
import com.cloudera.bigdata.analysis.dataload.exception.TableDisabledException;
import com.cloudera.bigdata.analysis.dataload.jaxb.SchemaUnmarshaller;
import com.cloudera.bigdata.analysis.dataload.source.HTableDefinition;
import com.cloudera.bigdata.analysis.dataload.source.HTableDefinitionImpl;
import com.cloudera.bigdata.analysis.dataload.source.InnerRecord;
import com.cloudera.bigdata.analysis.dataload.source.Record;
import com.cloudera.bigdata.analysis.generated.ColumnFamilyType;
import com.cloudera.bigdata.analysis.generated.MultiQualifierType;
import com.cloudera.bigdata.analysis.generated.QualifierType;
import com.cloudera.bigdata.analysis.generated.RowKeyFieldType;
import com.cloudera.bigdata.analysis.generated.TxtRecordType;
import com.cloudera.bigdata.analysis.index.util.RowKeyUtil;

public class LineRowMapper<KEYOUT, VALUEOUT> extends
    Mapper<LongWritable, Text, KEYOUT, VALUEOUT> {
  private static final Logger LOG = LoggerFactory
      .getLogger(LineRowMapper.class);
  public static final FileInfo DUMMY_FILE = new FileInfo(null);
  private ExecutorService execService;

  private TxtRecordType recordType;
  private String rowKeySeparater;
  private String cachedLine;
  private String[] fieldValues;
  private List<RowKeyFieldType> fieldSpecList;
  private List<ColumnFamilyType> cfSpecList;
  private boolean useIndex = false;
  private HashMap<String, HTableDefinition> definitionMap;
  private HTableDefinition cachedDefinition;
  private Configuration conf;
  private BlockingQueue<RecordWrapper> queue;
  private CountDownLatch countDownLatch;
  private AtomicBoolean atomicFlag = null;

  protected void setup(Context context) throws IOException {
    conf = context.getConfiguration();
    int lenQueue = conf.getInt(Constants.QUEUE_LENGTH_KEY,
        Constants.DEFAULT_QUEUE_LENGTH);
    this.queue = new LinkedBlockingQueue<RecordWrapper>(lenQueue);
    countDownLatch = new CountDownLatch(1);
    this.atomicFlag = new AtomicBoolean(false);
    execService = Executors.newFixedThreadPool(1);
    execService.submit(new Worker(0));

    Path[] files = DistributedCache.getLocalCacheFiles(conf);
    for (Path path : files) {
      if (path.getName().endsWith(conf.get(Constants.INSTANCE_DOC_NAME_KEY))) {
        conf.set(Constants.INSTANCE_DOC_PATH_KEY, path.toUri().toString());
        break;
      }
    }

    String instanceDoc = conf.get(Constants.INSTANCE_DOC_PATH_KEY);
    if (instanceDoc == null) {
      throw new IOException("Cannot find instanceDoc");
    }
    recordType = ((JAXBElement<TxtRecordType>) SchemaUnmarshaller.getInstance()
        .unmarshallDocument(TxtRecordType.class, instanceDoc)).getValue();

    rowKeySeparater = recordType.getRowKeySpec().getRowKeySeparater();
    fieldSpecList = recordType.getRowKeySpec().getRowKeyFieldSpec();

    cfSpecList = recordType.getColumnFamilySpec();

    // for index
    if (conf.getBoolean(Constants.BUILD_INDEX, false)) {
      useIndex = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("useIndex");
      }
    }
    if (definitionMap == null) {
      definitionMap = new HashMap<String, HTableDefinition>();
    }
    // maybe we have different policies to assemble the table name
    String tableName = conf.get(Constants.HBASE_TARGET_TABLE_NAME);
    cachedDefinition = definitionMap.get(tableName);
    if (cachedDefinition == null) {
      String splitPrefix = conf.get(Constants.SPLIT_KEY_PREFIXES, "");
      int splitSize = conf.getInt(Constants.SPLIT_SIZE_KEY, 1);
      if (LOG.isDebugEnabled()) {
        LOG.debug("tableName : " + tableName);
        LOG.debug("splitPrefix : " + splitPrefix);
        LOG.debug("splitSize : " + splitSize);
      }

      cachedDefinition = new HTableDefinitionImpl(tableName, cfSpecList,
          splitPrefix, splitSize, useIndex);

      definitionMap.put(tableName, cachedDefinition);
    }

    LOG.info("=====Setup Finish====");
  }

  protected void map(LongWritable key, Text value, Context contex)
      throws InterruptedException {
    cachedLine = value.toString();

    Record record = assembleRecord(cachedLine);

    RecordWrapper recordWrapper = new RecordWrapper(record, null);
    queue.put(recordWrapper);
  }

  protected void cleanup(Context context) {
    this.atomicFlag.compareAndSet(false, true);
    RecordWrapper dummy = new RecordWrapper(null, DUMMY_FILE);
    queue.offer(dummy);

    try {
      LOG.info("=====Waiting the Latch=====");
      waitUnLatch();
      LOG.info("=====Releaseing the Latch=====");

    } catch (InterruptedException e) {
      LOG.error("", e);
    }

    execService.shutdown();

    LOG.info("=====Clean Up=====");
  }

  public void releaseLatch(boolean isFetch) {
    LOG.info(Thread.currentThread().getName() + " is releasing latch");
    countDownLatch.countDown();
  }

  public void waitUnLatch() throws InterruptedException {
    countDownLatch.await();
  }

  public Record assembleRecord(String cachedLine) {
    Record record = null;
    if (!StringUtils.isEmpty(cachedLine)) {
      if (recordType.isUseSeparater()) {
        // fieldValues = cachedLine.split(recordType.getInputSeparater());
        // handle null fields
        String delimiter = recordType.getInputSeparater();
        fieldValues = StringUtils.splitByWholeSeparatorPreserveAllTokens(
            cachedLine, delimiter);
        if (LOG.isDebugEnabled()) {
          LOG.debug("cachedLine : " + cachedLine);
          LOG.debug("split delimiter : " + delimiter);
          LOG.debug("fieldValues size : " + fieldValues.length);
          LOG.debug("fieldValues : " + Arrays.toString(fieldValues));
        }
      }
      // buildIndex
      if (useIndex) {
        record = new InnerRecord(cachedDefinition, genRowKey(), getValueMap(),
            conf.getBoolean(Constants.WRITE_TO_WAL_KEY, false));
      } else {
        record = new InnerRecord(cachedDefinition, getRowKey(), getValueMap(),
            conf.getBoolean(Constants.WRITE_TO_WAL_KEY, false));
      }
      return record;
    }

    // TODO: null handling
    return null;
  }

  public byte[] genRowKey() {
    byte[] randomRowKeyPrefix = RowKeyUtil.genRandomRowKeyPrefix();
    byte[] rowKey = RowKeyUtil.genRowKey(randomRowKeyPrefix);
    return rowKey;
  }

  public byte[] getRowKey() {
    StringBuffer sb = new StringBuffer();
    for (RowKeyFieldType fieldSpec : fieldSpecList) {
      String fieldValue = null;
      if (recordType.isUseSeparater()) {
        fieldValue = fieldValues[fieldSpec.getFieldIndex()];
      } else {
        int startPos = fieldSpec.getStartPos();
        int length = fieldSpec.getLength();
        fieldValue = cachedLine.substring(startPos, startPos + length);
      }
      sb.append(fieldValue);
      sb.append(rowKeySeparater);
    }

    return sb.toString().getBytes();
  }

  public Map<byte[], Map<byte[], byte[]>> getValueMap() {
    HashMap<byte[], Map<byte[], byte[]>> cfMap = new HashMap<byte[], Map<byte[], byte[]>>();
    for (ColumnFamilyType cfType : cfSpecList) {
      cfType.getFamilyName();
      HashMap<byte[], byte[]> qualifierMap = new HashMap<byte[], byte[]>();
      for (QualifierType qType : cfType.getQualifierSpec()) {
        String qValue = null;
        if (recordType.isUseSeparater()) {
          qValue = fieldValues[qType.getFieldIndex()];
          if (LOG.isDebugEnabled()) {
            LOG.debug("qValue : fieldValues[" + qType.getFieldIndex() + "] = "
                + qValue);
          }
        } else {
          int startPos = qType.getStartPos();
          int length = qType.getLength();
          qValue = cachedLine.substring(startPos, startPos + length);
        }
        qualifierMap
            .put(qType.getQualifierName().getBytes(), qValue.getBytes());
      }
      for (MultiQualifierType qType : cfType.getMultiQualifierSpec()) {
        StringBuffer multiQualifyValue = new StringBuffer();
        if (recordType.isUseSeparater()) {
          String[] indexs = StringUtils.splitByWholeSeparator(
              qType.getFieldIndex(), ",");
          for (int i = 0; i < indexs.length - 1; i++) {
            multiQualifyValue.append(fieldValues[Integer.parseInt(indexs[i])]);
            multiQualifyValue.append("|");
          }
          multiQualifyValue.append(fieldValues[Integer
              .parseInt(indexs[indexs.length - 1])]);
          if (LOG.isDebugEnabled()) {
            LOG.debug("multiple qValue : " + multiQualifyValue);
          }
        } else {
          // TODO
          String startPos = qType.getStartPos();
          String length = qType.getLength();
          // TODO
          // qValue = cachedLine.substring(startPos, startPos + length);
        }
        qualifierMap.put(qType.getQualifierName().getBytes(), multiQualifyValue
            .toString().getBytes());
      }
      cfMap.put(cfType.getFamilyName().getBytes(), qualifierMap);
    }
    return cfMap;
  }

  public boolean hasFinished() {
    if (atomicFlag.get() && queue.isEmpty()) {
      return true;
    }
    return false;
  }

  public class Worker implements Runnable {
    private static final String WORKER_NAME_PREFIX = "UpdateWorker-";
    private ConcurrentMap<HTableDefinition, Boolean> openedTbls = new ConcurrentHashMap<HTableDefinition, Boolean>();
    private AtomicLong processedCount = new AtomicLong(0);
    private static final int MAX_RETRY = 3;

    private boolean enableCounter = true;
    private String name = null;
    private int id;
    private HBaseUpdater updateClient;
    private boolean hasRetryRecord = false;
    private int retryTimes = 0;

    public Worker(int id) {
      this.name = WORKER_NAME_PREFIX + id;
      this.id = id;
      this.updateClient = new HBaseUpdater(conf);
    }

    public String getName() {
      return name;
    }

    public String getThreadId() {
      return "" + id;
    }

    @Override
    public void run() {
      while (!hasFinished()) {
        RecordWrapper record;
        RecordWrapper previousRecord = null;
        try {
          if (!hasRetryRecord) {
            record = queue.take();
            previousRecord = record;
            retryTimes = 0;
          } else {
            hasRetryRecord = false;
            if (retryTimes <= MAX_RETRY) {
              record = previousRecord;
            } else {
              retryTimes = 0;
              continue;
            }
          }

          if (record != null) {

            Record r = record.getRecord();
            r.preConvert(this.id);
            Map<HTableDefinition, Row[]> ops = null;
            try {
              ops = r.convertToHBaseRecord();
              if (LOG.isDebugEnabled()) {
                LOG.debug("after convertToHBaseRecord");
              }
            } catch (IOException e) {
              e.getStackTrace();
            }

            if (ops == null) {
              throw new Exception(
                  "Failed to get Map<HTableDefinition, Row[]>, ops is null");
            } else {
              for (HTableDefinition tbl : ops.keySet()) {
                if (!openedTbls.containsKey(tbl)) {
                  openedTbls.put(tbl, true);
                }
              }
              updateClient.update(r, ops);
            }

            if (enableCounter) {
              long count = processedCount.incrementAndGet();
              if (count % 10000 == 0) {
                LOG.info("===== Processed " + count + " rows.");
              }
            }
          }
        } catch (TableDisabledException e) {

        } catch (InterruptedException e) {
          e.getStackTrace();
        } catch (IOException e) {
          hasRetryRecord = true;
          retryTimes++;
        } catch (Exception e) {
          hasRetryRecord = true;
          retryTimes++;
        }
      }

      try {
        updateClient.flush();
        LOG.info(Thread.currentThread().getName() + " is closing");
        updateClient.close();
      } catch (IOException e) {
        LOG.error("updateClient flush Error", e);
      }

      releaseLatch(false);
    }
  }
}
