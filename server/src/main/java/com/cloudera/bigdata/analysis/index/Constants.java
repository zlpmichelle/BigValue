package com.cloudera.bigdata.analysis.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.index.util.IndexUtil;

public class Constants {

  public final static String PROP_PREFIX = "props.";

  /**
   * Default block size for an HFile.
   */
  public final static int HFILE_DEFAULT_BLOCKSIZE = 64 * 1024;

  public final static String DATALOAD_MODE = "dataload.mode";
  public final static String DATALOAD_MAPRED_MODE = "mapred";
  public final static String DATALOAD_CLIENT_MODE = "local";

  public final static String FILE_NUM_KEY = "dataload.source.fileNum";
  public final static int DEFAULT_FILE_NUM = 64;

  public final static String IN_MEM_FILENAME_PREFIX = "InMem-";

  public final static String PROPERTY_FOLDER_KEY = "dataload.source.propertyFolder";

  public final static String FETCH_PARALLEL_KEY = "dataload.client.fetchParallel";
  public final static int DEFAULT_FETCH_PARALLEL = 1;

  public final static String THREADS_PER_MAPPER_KEY = "dataload.client.threadsPerMapper";
  public final static int DEFAULT_THREADS_PER_MAPPER = 1;

  public final static String QUEUE_LENGTH_KEY = "dataload.client.queueLength";
  public final static int DEFAULT_QUEUE_LENGTH = 1000;

  public final static String STREAMING_FETCH_KEY = "dataload.source.streaming.fetch";
  public final static boolean DEFAULT_STREAM_FETCH = false;

  public final static String DATASOURCE_CLASS_KEY = "dataload.source.dataSourceClass";

  public static final String HDFSDIRS = "dataload.source.hdfsDirs";

  public final static String FILEPARSER_CLASS_KEY = "fileParserClass";

  public final static String MAPRED_JOB_TRACKER_KEY = "mapred.job.tracker";

  public final static String MAPRED_JOBTRACKER_PORT_KEY = "mapred.job.tracker.port";

  public final static int DEFAULT_JOBTRACKER_PORT = 54311;

  public final static String DATALOAD_SOURCE_FTP_DIR = "dataload.source.ftpDirs";

  public final static String RECORD_NUM_PER_FILE_KEY = "dataload.source.recordNumPerFile";
  public final static long DEFAULT_RECORD_NUM_PER_FILE = 10000L;

  public final static String COLUMN_FAMILIES_KEY = "hbase.table.columnFamilies";

  public final static String SPLIT_KEY_PREFIXES = "hbase.table.splitKeyPrefixes";

  public final static String SPLIT_SIZE_KEY = "hbase.table.splitSize";

  public final static String COLUMN_REPLICATION_KEY = "hbase.table.columnReplication";

  public final static String DELIMITER = "|";

  public final static String DIRS_SEPARATOR = ",";

  public final static String INDEX_SEPARATER = "#";

  public final static String COLUMN_KEY_DELIMITER = ":";

  public final static String BLOB_FAMILY = "b";

  public final static String BLOB_QUALIFIER = "q";

  public final static String COMMON_FAMILY = "f";

  public final static String COMMON_QUALIFIER = "c";

  public final static String CREATE_TABLE_KEY = "createTableIfNotExist";
  public final static boolean DEFAULT_CREATE_TABLE = true;

  public final static String WRITE_BUFFER_SIZE_KEY = "hbase.table.writeBufferSize";
  public final static int DEFAULT_WRITE_BUFFER_SIZE = 6;

  public final static String AUTO_FLUSH_KEY = "hbase.table.autoFlush";
  public final static boolean DEFAULT_AUTO_FLUSH = false;

  public final static String WRITE_TO_WAL_KEY = "hbase.table.writeToWAL";
  public final static boolean DEFAULT_WRITE_TO_WAL = false;

  public static final String FS_DEFAULT_NAME_KEY = "fs.default.name";

  public static final String DATALOAD_ONLY_A_LARGE_FILE_KEY = "dataload.only.a.large.file";

  public static final boolean DEFAULT_DATALOAD_SMALL_FILE = true;

  public final static String FILEOBJECT_CLASS_KEY = "mapred.fileobject.class";

  public final static String LINES_PER_MAP_KEY = "mapred.inputformat.linespermap";

  public final static String TASK_TIMEOUT_KEY = "mapred.task.timeout";

  public final static String MAPPER_SPECULATIVE_KEY = "mapred.map.tasks.speculative.execution";

  public final static String REDUCER_SPECULATIVE_KEY = "mapred.reduce.tasks.speculative.execution";

  public final static String IN_MEMORY_FILENAME_PREFIX = "INMEMORY-";

  public final static String PARSER_TYPE_KEY = "dataload.source.parserType";

  public final static String INSTANCE_DOC_PATH_KEY = "dataload.source.instanceDocPath";

  public final static String INSTANCE_DOC_NAME_KEY = "instanceDocName";

  public final static String PARSER_READER_BUF_SIZE = "parser.reader.buf.size";

  public final static String PARSER_READER_BUF_SIZE_KEY = "readerBufferSize";

  public final static int DEFAULT_PARSER_READER_BUF_SIZE = 8192;

  public final static byte[] EMPTY_BYTE_ARRAY = new byte[] {};

  public static final String FTP_SPLIT = ":";

  public static final String COLUMN_ENTRY_SEPARATOR = "|";

  public static final String COLUMN_KEY_VALUE_SEPARATOR = "=";

  public static final String ROW_KEY_NAME = "rowkey";

  public static final String OPEN_PARENTHESIS = "(";

  public static final String CLOSE_PARENTHESIS = ")";

  public static final String COMMA = ",";

  public static final String SINGLE_QUOTATION_MARK = "'";

  // ConfigReader constants Start
  public static final String START_WITH_CHARACTER = "#";

  public static final String TOKEN_SPLIT_CHARACTER = "=";

  public static final String CELL_SPLIT_CHARACTER = ",";

  public static final String FAMILY_QUALIFIER_SPLIT_CHARACTER = ".";

  // from hbase source begin

  public static final String HBASE_SOURCE_TABLE_NAME = "hbase.source.table.name";

  public static final String TO_BE_CLEANED_ROWKEY_RANGE_SPEC = "to.be.cleaned.rowkey.range.spec";

  public static final String TO_BE_CLEANED_CELL_SPEC = "to.be.cleaned.cell.spec";

  // from hbase source end

  // from hdfs source begin, bulkload source definition
  public static final String IDP_HBASE_MASTER_IPADDRESS = "idp.hbase.master.ip.address";
  public static final String HDFS_SOURCE_FILE_INPUT_PATH = "hdfs.source.file.input.path";
  public static final String HDFS_SOURCE_FILE_ENCODING = "hdfs.source.file.encoding";
  public static final String DEFAULT_TEXT_ENCODING = "UTF-8";
  public static final String HDFS_SOURCE_FILE_RECORD_FIELDS_DELIMITER = "hdfs.source.file.record.fields.delimiter";
  public static final String DEFAULT_FIELD_DELIMITER = ",";
  public static final String HDFS_SOURCE_FILE_RECORD_FIELDS_NUMBER = "hdfs.source.file.record.fields.number";
  public static final String HDFS_SOURCE_FILE_RECORD_FIELDS_TYPE_INT = "hdfs.source.file.record.fields.type.int";
  public static final String HDFS_SOURCE_FILE_RECORD_FIELD_NAME_PREFIX = "f";
  public static final String DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER = ":";
  public static final String HDFS_SOURCE_FILE_RECORD_DEFAULT_FIELD_TYPE = "STRING";
  public static final String HDFS_SOURCE_FILE_RECORD_INT_FIELD_TYPE = "INT";

  // to hbase target end, bulkload target definition
  public static final String HBASE_TARGET_TABLE_NAME = "hbase.target.table.name";
  public static final String HBASE_GENERATED_HFILES_OUTPUT_PATH = "hbase.generated.hfiles.output.path";
  public static final String HBASE_TARGET_WRITE_TO_WAL_FLAG = "hbase.target.write.to.wal.flag";
  public static final String HBASE_TARGET_TABLE_CELL_SPEC = "hbase.target.table.cell.spec";

  // bulkload stage definition
  public final static String BUILD_INDEX = "buildIndex";
  public final static String REGION_QUANTITY = "regionQuantity";
  public final static String INDEX_CONF_FILE_NAME = "indexConfFileName";
  // coprocessor path
  public final static String HBASE_COPROCESSOR_LOCATION = "hbaseCoprocessorLocation";
  public final static String ONLY_GENERATE_SPLITKEYSPEC = "onlyGenerateSplitKeySpec";

  public final static String PRE_CREATE_REGIONS = "preCreateRegions";
  public final static String ROWKEY_PREFIX = "rowkeyPrefix";
  public final static String RECORDS_NUM_PER_REGION = "recordsNumPerRegion";
  public static final String HBASE_TARGET_TABLE_SPLIT_KEY_SPEC = "hbase.target.table.split.key.spec";
  public final static String EXTENDEDHBASEROWCONVERTER_CLASS_KEY = "extendedHbaseRowConverterClass";
  public static final String INPUT_SPLIT_SIZE = "inputSplitSize";
  public final static String IMPORT_DATE = "importDate";
  public final static String VALIDATOR_CLASS = "validatorClass";
  public final static String CREATE_MALFORMED_TABLE = "createMalformedTable";
  public final static String NATIVETASK_ENABLED = "nativeTaskEnabled";

  public final static byte B_IDX_ROWKEY_DELIMITER = (byte) (",").charAt(0);
  public final static byte[] IDX_ROWKEY_DELIMITER = Bytes.toBytes(",");
  public final static int ROWKEY_PREFIX_LENGTH = 4;
  /** the string pattern of randdom row key */
  public final static String ROWKEY_PREFIX_PATTERN = "%0"
      + ROWKEY_PREFIX_LENGTH + "d";
  /** the max int value of random row key prefix */
  public final static int ROWKEY_PREFIX_MAX_VALUE = (int) Math.pow(10,
      ROWKEY_PREFIX_LENGTH);
  public final static byte[] FAMILY_F = Bytes.toBytes("f");
  public final static byte[] FAMILY_I = Bytes.toBytes("i");
  public final static byte B_HBASE_TABLE_DELIMITER = (byte) ("|").charAt(0);
  public final static byte[] HBASE_TABLE_DELIMITER = Bytes.toBytes("|");
  public final static int QUERY_MAX_RESULT_LIMIT = 1000000;
  public final static long PROTOCOL_VERSION = 1L;
  public final static String CURRENT_PRO_HDFS = "/user/hbase/conf/current.properties";
  public final static String INDEX_CONF_DIR_HDFS = "/user/hbase/conf";
  public final static String INDEX_XSD_HDFS = "/user/hbase/conf/index-conf.xsd";
  public final static String CORE_SITE_XML = "/etc/hadoop/conf/core-site.xml";
  // for load
  public static String CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD = IndexUtil
      .getCurrentIndexConfNameForLoad(CURRENT_PRO_HDFS);

  public static String CURRENT_TABLE = IndexUtil
      .getCurrentTableName(CURRENT_PRO_HDFS);

  // for common
  public static String CURRENT_INDEX_CONF_FILE_NAME = IndexUtil
      .getCurrentIndexConfNameForQuery(CURRENT_TABLE, new Path(
          INDEX_CONF_DIR_HDFS));

  /** single wildcard character for fuzzy query */
  public final static String S_SINGLE_WILDCARD = "\\?";
  /** single wildcard pattern for fuzzy query */
  public final static String S_SINGLE_WILDCARD_PATTERN = ".";
  /** multiple wildcard character for fuzzy query */
  public final static String S_MULTIPLE_WILDCARD = "\\*";
  /** multiple wildcard pattern for fuzzy query */
  public final static String S_MULTIPLE_WILDCARD_PATTERN = ".*";

  public final static boolean ENABLE_STATISTICS = false;

  // TODO need to set
  public final static String P_SERVER_SEARCH_ENABLE_STATISTICS = "server.search.enableStatistics";

  public final static boolean P_SERVER_SEARCH_ENABLE_BLOCK_CACHE_FOR_SEARCH = false;

  /**
   * row key length of table CaptureRecord. 1 is delimiter, 16(128 bit) is UUID
   * length
   */
  public final static int CR_ROW_KEY_LENGTH = ROWKEY_PREFIX_LENGTH + 1 + 36;
}
