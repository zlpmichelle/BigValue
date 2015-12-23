package com.cloudera.bigdata.analysis.datagen;

import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

/**
 * For RandomDateGenerator, you must specify as following:
 *  <start> <end> <format>
 */
public class RandomDateGenerator extends FieldGenerator {
  
  private static final Logger LOG = LoggerFactory.getLogger(RandomDateGenerator.class);
  
  private long startTime;
  private long endTime;
  private String startString;
  private String endString;
  private DateTimeFormatter formatter;
  private Random random;
  
  public RandomDateGenerator(ColumnSpec columnSpec){
    this(null, columnSpec);
  }
  
  public RandomDateGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
    
    startString = columnSpec.getStart();
    endString = columnSpec.getEnd();
    random = new Random(new DateTime().getMillis());
    formatter = DateTimeFormat.forPattern(columnSpec.getPattern());
    try{
      startTime = formatter.parseDateTime(startString).getMillis();
      endTime = formatter.parseDateTime(endString).getMillis();
    }catch(IllegalArgumentException e){
      LOG.error("", e);
      System.exit(1);
    }
    if(endTime<startTime){
      LOG.error("The start time is bigger than the end time.");
      System.exit(1);
    }
  }

  @Override
  public String generate() {
    if(startTime == endTime){
      return startString;
    }else{
      long interval = Math.abs(random.nextLong())%(endTime - startTime);
      return formatter.print(startTime + interval);
    }
  }

}
