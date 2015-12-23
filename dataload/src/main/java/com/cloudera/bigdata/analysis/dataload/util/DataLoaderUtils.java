package com.cloudera.bigdata.analysis.dataload.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public class DataLoaderUtils {

  private static final Logger LOGGER = Logger.getLogger(DataLoaderUtils.class);

  public static Properties loadConfig(String config) {
    Properties props = new Properties();
    try {
      props.load(new FileInputStream(config));
      return props;
    } catch (FileNotFoundException fnfe) {
      LOGGER.error("File does not exist: " + config, fnfe);
      throw new RuntimeException(fnfe);
    } catch (IOException ioe) {
      LOGGER.error("Error in reading " + config, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public static Map<String, String> readConfig(String config) {
    Map<String, String> confMap = new HashMap<String, String>();
    BufferedReader br = null;
    try {
      InputStreamReader isr = new InputStreamReader(new FileInputStream(config));
      br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.startsWith(Constants.START_WITH_CHARACTER)) {
          continue;
        }
        String[] tokens = line.split(Constants.TOKEN_SPLIT_CHARACTER);
        if (tokens.length != 2) {
          continue;
        }
        // construct confMap with configuration file
        confMap.put(tokens[0].trim(), tokens[1].trim());
      }
      br.close();
    } catch (Exception e) {
      ETLException.handle(e);
    } finally {
      try {
        if (br != null) {
          br.close();
        }
      } catch (Exception e) {

      }
    }
    return confMap;
  }
}
