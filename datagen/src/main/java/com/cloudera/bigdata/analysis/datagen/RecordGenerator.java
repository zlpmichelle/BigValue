package com.cloudera.bigdata.analysis.datagen;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.bigdata.analysis.dataload.util.Util;

public abstract class RecordGenerator {
  private final static Map<Class<? extends RecordGenerator>, RecordGenerator> GENERATOR_MAP = 
      new HashMap<Class<? extends RecordGenerator>, RecordGenerator>();
  
  public static void define(Class<? extends RecordGenerator> clazz, RecordGenerator generator){
    GENERATOR_MAP.put(clazz, generator);
  }
  
  public static RecordGenerator getInstance(Class<? extends RecordGenerator> clazz){
    RecordGenerator generator = GENERATOR_MAP.get(clazz);
    if(generator==null){
      Util.forceInit(clazz.getName());
    }
    return GENERATOR_MAP.get(clazz);
  }
  
  public abstract String getReference(String fieldName);
  
  public abstract void loadGenerator(String instanceDoc);
  
  public abstract Record generate();
}
