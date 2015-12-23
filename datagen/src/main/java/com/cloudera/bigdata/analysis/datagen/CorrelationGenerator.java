package com.cloudera.bigdata.analysis.datagen;

import com.cloudera.bigdata.analysis.generated.ColumnRef;
import com.cloudera.bigdata.analysis.generated.ColumnSpec;
import com.cloudera.bigdata.analysis.generated.OpType;

public class CorrelationGenerator extends FieldGenerator {

  public CorrelationGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
  }
  
  @Override
  public String generate() {
    ColumnRef columnRef = columnSpec.getRef();
    String refValue = wrapperGenerator.getReference(columnRef.getRefColumn());
    OpType opType = columnRef.getRefOp();
    switch (opType) {
    case IDENTITY:
      return refValue;
    case SUBSTRING:
      return refValue.substring(columnRef.getRefStart(), columnRef.getRefEnd());
    case CONCACT:
      return refValue;
    case DIGEST:
      return Md5Util.getMD5Str(refValue);
    default:
      break;
    }
    
    return refValue;
  }

}
