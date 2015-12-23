package com.cloudera.bigdata.analysis.datagen;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class RandomLongGenerator extends FieldGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(RandomLongGenerator.class);
  
  private long min = 0L;
  private long max = 10000L;

  private Random rand = new Random();
  
  public RandomLongGenerator(ColumnSpec columnSpec){
    this(null, columnSpec);
  }
  
  public RandomLongGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
    
    try{
      if(columnSpec.getStart()!=null){
        min = Long.parseLong(columnSpec.getStart());
      }
      
      if(columnSpec.getEnd()!=null){
        max = Long.parseLong(columnSpec.getEnd());
      }
    }catch (NumberFormatException e) {
      LOG.error("Format exception for " + this, e);
    }
   
  }
  
  @Override
  public String generate() {
    return String.valueOf(Math.abs(rand.nextInt()) % (max - min) + min);
  }

}
