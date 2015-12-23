package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.bigdata.analysis.dataload.exception.ParseException;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;

/**
 * One FileParser parse one file
 * */
public abstract class FileParser {
  /** Initialize this data source */
  public abstract void init(InputStream is, FileObject file, Configuration conf)
      throws IOException;

  /** Get next record from the file */
  public abstract Record getNext() throws IOException, ParseException;

  /** Close the FileParser */
  public abstract void close() throws IOException;

  public static FileParser getFileParser(ParserType type) throws ParseException {
    FileParser parser = null;
    switch (type) {
    case TEXT:
      parser = new TextFileParser();
      break;
    case CSV:
      parser = new TextFileParser();
      break;
    // current, no strong requirement for XML and JSON
    case XML:
      break;
    case JSON:
      break;
    case INMEMORY:
      parser = new InMemoryFileParser();
      break;
    default:
      throw new ParseException("Unsupported Type: " + type, null);
    }
    return parser;
  }

  public static enum ParserType {
    TEXT("text"), CSV("csv"), XML("xml"), JSON("json"), INMEMORY("inmemory");

    private String typeString;

    ParserType(String typeString) {
      this.typeString = typeString;
    }

    public String toString() {
      return typeString;
    }

    public static ParserType formValue(String typeString) {
      if (TEXT.toString().equalsIgnoreCase(typeString)) {
        return TEXT;
      }
      if (CSV.toString().equalsIgnoreCase(typeString)) {
        return CSV;
      }
      if (XML.toString().equalsIgnoreCase(typeString)) {
        return XML;
      }
      if (JSON.toString().equalsIgnoreCase(typeString)) {
        return JSON;
      }
      if (INMEMORY.toString().equalsIgnoreCase(typeString)) {
        return INMEMORY;
      } else
        return null;
    }
  }
}
