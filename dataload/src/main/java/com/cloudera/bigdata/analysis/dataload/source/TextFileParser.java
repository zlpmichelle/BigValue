package com.cloudera.bigdata.analysis.dataload.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBElement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ParseException;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.jaxb.SchemaUnmarshaller;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;
import com.cloudera.bigdata.analysis.generated.ColumnFamilyType;
import com.cloudera.bigdata.analysis.generated.MultiQualifierType;
import com.cloudera.bigdata.analysis.generated.QualifierType;
import com.cloudera.bigdata.analysis.generated.RowKeyFieldType;
import com.cloudera.bigdata.analysis.generated.TxtRecordType;

/**
 * TextFileParser is used to parse the structured text files. Currently, it
 * assumes each line in the file is separated by the default "\n". There would
 * be two typical parsing style for the text file. One style is: each field is
 * represented by the fixed length; another style is: all the fields are
 * separated by a fixed separator. TextFileParser is not thread-safe.
 */
public class TextFileParser extends FileParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(TextFileParser.class);

  private Configuration conf;

  private TxtRecordType recordType;
  private String rowKeySeparater;
  private List<RowKeyFieldType> fieldSpecList;
  private List<ColumnFamilyType> cfSpecList;

  private String cachedLine;
  private String[] fieldValues;
  private BufferedReader reader;
  private int readerBufSize = 4096;
  private boolean useIndex = false;

  private HashMap<String, HTableDefinition> definitionMap;
  private HTableDefinition cachedDefinition;

  public TextFileParser() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating the parser...");
    }
    definitionMap = new HashMap<String, HTableDefinition>();
  }

  /**
   * Invoked before parsing file
   */
  @Override
  public void init(InputStream is, FileObject file, Configuration configuration)
      throws IOException {
    this.conf = configuration;
    readerBufSize = conf.getInt(Constants.PARSER_READER_BUF_SIZE_KEY,
        Constants.DEFAULT_PARSER_READER_BUF_SIZE);

    // TODO: add extra char set encoding
    reader = new BufferedReader(new InputStreamReader(is), readerBufSize);
    String instanceDoc = conf.get(Constants.INSTANCE_DOC_PATH_KEY);
    LOG.info("instanceDocPath: " + instanceDoc);
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
  }

  @Override
  public Record getNext() throws IOException, ParseException {
    cachedLine = reader.readLine();
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
        record = new InnerRecord(cachedDefinition, CommonUtils.genRowKey(),
            getValueMap(), conf.getBoolean(Constants.WRITE_TO_WAL_KEY, false));
      } else {
        record = new InnerRecord(cachedDefinition, getRowKey(), getValueMap(),
            conf.getBoolean(Constants.WRITE_TO_WAL_KEY, false));
      }
      return record;
    }

    // TODO: null handling
    return null;
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

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }
}
