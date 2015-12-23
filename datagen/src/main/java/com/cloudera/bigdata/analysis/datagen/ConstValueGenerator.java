package com.cloudera.bigdata.analysis.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class ConstValueGenerator extends FieldGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(ConstValueGenerator.class);
	private String value;
	
	public ConstValueGenerator(){
		
	}
	
	public ConstValueGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
	  super(recordGenerator, columnSpec);
		value = columnSpec.getValue();
		
		if(value==null){
		  LOG.error("No constant value found for " + this);
		}
	}
	
	@Override
	public String generate() {
		return value;
	}
	
}
