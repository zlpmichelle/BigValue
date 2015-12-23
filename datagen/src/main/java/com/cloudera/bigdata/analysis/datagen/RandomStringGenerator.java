package com.cloudera.bigdata.analysis.datagen;

import com.cloudera.bigdata.analysis.dataload.util.RandomByteIterator;
import com.cloudera.bigdata.analysis.generated.ColumnSpec;
import com.cloudera.bigdata.analysis.generated.FieldPattern;
import com.cloudera.bigdata.analysis.generated.RandomPattern;

public class RandomStringGenerator extends FieldGenerator {
  
  private FieldPattern fieldPattern;
  
  public RandomStringGenerator(
      RecordGenerator recordGenerator,
      ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
    
    fieldPattern = adapter(columnSpec.getRandomPattern());
  }
  
  private FieldPattern adapter(RandomPattern randomPattern){
    if(randomPattern==null){
      return FieldPattern.ALPHABET;
    }else{
      return FieldPattern.fromValue(randomPattern.name());
    }
  }

  @Override
  public String generate() {
    RandomByteIterator byteIterator = 
        new RandomByteIterator((int)columnSpec.getLength(), fieldPattern);
    return byteIterator.toString();
  }

}
