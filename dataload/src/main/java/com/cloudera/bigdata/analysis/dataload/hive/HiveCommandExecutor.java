package com.cloudera.bigdata.analysis.dataload.hive;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.util.DataLoaderUtils;

public class HiveCommandExecutor {
  private static final Logger LOGGER = Logger
      .getLogger(HiveCommandExecutor.class);
  private static String HIVE_OPTION = "-e";
  public static final String[] HIVE_EXECUTION_PATTERN = {
      "FAILED: Error in semantic analysis", "FAILED: Execution Error" };
  private Pattern[] hiveExecutionExceptionPatterns;
  private HiveCommand executeCommand;
  private Properties props;

  public HiveCommandExecutor(String configFile) {
    this.props = DataLoaderUtils.loadConfig(configFile);
    initializePatterns();
    populateHiveCommand();
  }

  public static void main(String[] args) throws IOException {
    HiveCommandExecutor hiveCommandExecutor = new HiveCommandExecutor(
        "etl-hive-conf.properties");
    hiveCommandExecutor.executeHiveCommands();
  }

  public void executeHiveCommands() throws IOException {

    LOGGER.info("Executing Hive Script:\n" + executeCommand.toString());
    Runtime rt = Runtime.getRuntime();
    rt.availableProcessors();
    Process process = rt.exec(new String[] {
        executeCommand.getHiveExecutable(), executeCommand.getHiveOption(),
        executeCommand.getHiveSetting() + executeCommand.getHiveScript() });
    String line;
    InputStream es = process.getErrorStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(es));
    while ((line = br.readLine()) != null) {
      LOGGER.info(line);
      if (matchHiveExecutionExceptionPattern(line)) {
        LOGGER.error(line);
        throw new RuntimeException(line);
      }
    }
    br.close();
  }

  private boolean matchHiveExecutionExceptionPattern(String log) {
    for (Pattern pattern : hiveExecutionExceptionPatterns) {
      Matcher matcher = pattern.matcher(log);
      while (matcher.find()) {
        return true;
      }
    }
    return false;
  }

  private void initializePatterns() {
    hiveExecutionExceptionPatterns = new Pattern[HIVE_EXECUTION_PATTERN.length];
    for (int i = 0; i < hiveExecutionExceptionPatterns.length; i++) {
      hiveExecutionExceptionPatterns[i] = Pattern
          .compile(HIVE_EXECUTION_PATTERN[i]);
    }
  }

  private void populateHiveCommand() {
    executeCommand = new HiveCommand(props.getProperty("hiveExecutable"),
        HIVE_OPTION, props.getProperty("hiveSetting"),
        props.getProperty("cleanSql"));
  }
}
