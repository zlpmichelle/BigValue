package com.cloudera.bigdata.analysis.datagen;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class RandomPhoneGenerator extends FieldGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(RandomPhoneGenerator.class);
  
  private static final String[] PHONE_PREFIXES = new String[]{
    "130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
    "150", "151", "152", "153", "155", "156", "157", "158", "159",
    "180", "181", "182", "183", "184", "185", "186", "187", "188", "189"
  };
  
  private List<String> prefixList;
  private long min;
  private String minPrefix;
  private int minIndex;
  private long max;
  private String maxPrefix;
  private int maxIndex;
  
  public RandomPhoneGenerator(ColumnSpec columnSpec){
    this(null, columnSpec);
  }
  
  public RandomPhoneGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
    prefixList = new ArrayList<String>();
    for(int i=0;i<PHONE_PREFIXES.length;i++){
      prefixList.add(PHONE_PREFIXES[i]);
    }
    try{
      min = Long.parseLong(columnSpec.getStart());
      minPrefix = columnSpec.getStart().substring(0, 3);
      minIndex = prefixList.indexOf(minPrefix);
      max = Long.parseLong(columnSpec.getEnd());
      maxPrefix = columnSpec.getEnd().substring(0, 3);
      maxIndex = prefixList.indexOf(maxPrefix);
      
      if(min>max){
        LOG.warn("start is larger than end");
        System.out.println(Thread.currentThread() + " is quitting.");
        System.exit(1);
      }
    }catch(NumberFormatException e){
      LOG.error("" + e);
    }
  }
  
  @Override
  public String generate() {
    int randomIndex = maxIndex;
    if(maxIndex>minIndex){
      randomIndex = random.nextInt(maxIndex - minIndex) + minIndex;
    }
    
    long value = Math.abs(random.nextLong())%(max-min) + min;
    StringBuilder builder = new StringBuilder();
    builder.append(value);
    builder.replace(0, 3, PHONE_PREFIXES[randomIndex]);
    return builder.toString();
  }

}
