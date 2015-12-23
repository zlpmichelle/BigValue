package com.cloudera.bigdata.analysis.datagen;

import java.util.Random;

import org.joda.time.DateTime;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public abstract class FieldGenerator {
  protected Random random;
  protected ColumnSpec columnSpec;
  protected RecordGenerator wrapperGenerator;
  
  public FieldGenerator(){
    random = new Random(new DateTime().getMillis());
  }
  
  public FieldGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    this();
    this.columnSpec = columnSpec;
    this.wrapperGenerator = recordGenerator;
  }
  
  public String fieldName(){
    return columnSpec.getName();
  }
  
  public static FieldGenerator createValueGenerator(
      RecordGenerator recordGenerator, 
      ColumnSpec columnSpec){
    
    switch (columnSpec.getRandomType()) {
    case DATETIME:
      return new RandomDateGenerator(recordGenerator, columnSpec);
    case LONG:
      return new RandomLongGenerator(recordGenerator, columnSpec);
    case PHONE:
      return new RandomPhoneGenerator(recordGenerator, columnSpec);
    case CORRELATE:
      return new CorrelationGenerator(recordGenerator, columnSpec);
    case CONSTANT:
      return new ConstValueGenerator(recordGenerator, columnSpec);
    case STRING:
      return new RandomStringGenerator(recordGenerator, columnSpec);
    case URL:
      return new RandomUrlGenerator(recordGenerator, columnSpec);
    case UDF:
      return getCustomFieldGenerator(recordGenerator, columnSpec);
      default:
      return null;
    }
  }
  
  public String toString(){
    return columnSpec.getName();
  }
  
  public static FieldGenerator getCustomFieldGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    try {
      return (FieldGenerator) Class.forName(columnSpec.getUdfName())
          .getConstructor(RecordGenerator.class, ColumnSpec.class)
          .newInstance(recordGenerator, columnSpec);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
  
  public abstract String generate();
}
