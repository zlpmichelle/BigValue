package com.cloudera.bigdata.analysis.datagen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.jaxb.SchemaUnmarshaller;
import com.cloudera.bigdata.analysis.generated.ColumnSpec;
import com.cloudera.bigdata.analysis.generated.RandomType;
import com.cloudera.bigdata.analysis.generated.RecordSpec;

public class TextRecordGenerator extends RecordGenerator{
  private static final Logger LOG = LoggerFactory.getLogger(TextRecordGenerator.class);
  
  private RecordSpec recordSpec;
  private List<FieldGenerator> fieldGenerators;
  private Set<String> referenceSet;
  private Map<String, String> referenceValueMap;
  
  public TextRecordGenerator(){
    LOG.info("Creating TextRecordGenerator");
    referenceValueMap = new HashMap<String, String>();
  }

  @Override
  public Record generate() {
    StringBuilder sb = new StringBuilder();
    int generatorNum = fieldGenerators.size();
    for(int i=0;i<generatorNum;i++){
      String value = null;
      try{
        value = fieldGenerators.get(i).generate();
      }catch (NullPointerException e) {
        LOG.error(fieldGenerators.toString(), e);
      }
      
      if(referenceSet.contains(fieldGenerators.get(i).fieldName())){
        referenceValueMap.put(fieldGenerators.get(i).fieldName(), value);
      }
      sb.append(value);
      if(i<generatorNum -1){
        sb.append(recordSpec.getSeparator());
      }
    }
    
    return new TextRecord(sb.toString(), sb.length());
  }
  
  @Override
  public String getReference(String fieldName){
    return referenceValueMap.get(fieldName);
  }

  @Override
  public void loadGenerator(String instanceDoc) {
	  JAXBElement<RecordSpec> jaxbElement = 
	        SchemaUnmarshaller.getInstance().unmarshallDocument(RecordSpec.class, instanceDoc);
	  recordSpec = jaxbElement.getValue();
	  referenceSet = new HashSet<String>();
	  List<ColumnSpec> columnSpecs = recordSpec.getColumn();
	  
	  // move all dependent columns before others
	  int i=0, j=columnSpecs.size()-1;
    while(true){
      while(i<=j && columnSpecs.get(i).getRef()==null){
        i++;
      }
      while(j>=i && columnSpecs.get(j).getRef()!=null){
        j--;
      }
      if(i>j){
        break;
      }
      ColumnSpec temp = columnSpecs.get(i);
      columnSpecs.set(i, columnSpecs.get(j));
      columnSpecs.set(j, temp);
      
      i++;
      j--;
    }
	  fieldGenerators = new ArrayList<FieldGenerator>(columnSpecs.size());
	  
	  //TODO: the reference DAG should be analyzed
	  for(ColumnSpec columnSpec:columnSpecs){
	    if(columnSpec.getRandomType()==RandomType.CORRELATE){
	      referenceSet.add(columnSpec.getRef().getRefColumn());
	    }
	    FieldGenerator generator = FieldGenerator.createValueGenerator(this, columnSpec);
	    if(generator==null){
	      LOG.error(Thread.currentThread().getName() + "Cannot create generator for " + fieldGenerators);
	      Thread.currentThread().interrupt();
	    }
	    fieldGenerators.add(generator);
	  }
  }

}
