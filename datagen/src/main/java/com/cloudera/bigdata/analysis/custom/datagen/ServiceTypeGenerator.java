package com.cloudera.bigdata.analysis.custom.datagen;

import com.cloudera.bigdata.analysis.datagen.CustomFieldGenerator;
import com.cloudera.bigdata.analysis.datagen.RecordGenerator;
import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class ServiceTypeGenerator extends CustomFieldGenerator {

  public ServiceTypeGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
  }
  
  @Override
  public String generate() {
    return "DummyServiceType";
  }

}
