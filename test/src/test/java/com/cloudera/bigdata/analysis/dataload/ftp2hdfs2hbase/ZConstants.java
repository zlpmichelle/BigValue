package com.cloudera.bigdata.analysis.dataload.ftp2hdfs2hbase;

public class ZConstants {
  public final static String TABLENAME = "tableName";

  public static final String MAPREDSERVER = "mapredserver";
  public static final String MAPREDPORT = "mapredport";
  public final static String MAPREDNUM = "mapredNum";

  public final static String JOBNAME = "jobname";
  public final static String INPUTDIR = "inputDir";
  public final static String OUTPUTDIR = "outputDir";
  public final static String FAMILY = "family";
  public final static String INPUTSIZE = "inputSize";

  public final static String DAYORHOUR = "dayOrHour";
  public final static String TIME = "time";

  public final static String MSISDN = "msisdn";
  public final static String STARTTIME = "starttime";
  public final static String ENDTIME = "endtime";
  public final static String QUERYNUM = "querynum";

  public final static String BATCHSIZE = "batchSize";

  public final static String SPLD1TABLENAME = "spld1";
  public final static String SPLD2TABLENAME = "spld2";
  public final static String CODEIDTABLENAME = "codeid";

  public final static int SPDOMAINL1INT = 1;
  public final static int SPDOMAINL2INT = 2;
  public final static int CODEIDINT = 3;

  public enum TYPE {
    SPDOMAINL1, SPDOMAINL2, CODEID
  }

  public enum COUNTER {
    RECORDCOUNT, FILECOUNT
  }
}
