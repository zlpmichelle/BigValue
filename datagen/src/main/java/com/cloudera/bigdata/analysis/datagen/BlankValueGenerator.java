package com.cloudera.bigdata.analysis.datagen;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class BlankValueGenerator extends FieldGenerator {
	private static final String EMPTY = "";
	public BlankValueGenerator(){
		
	}
	
	public BlankValueGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
		super(recordGenerator, columnSpec);
	}

	@Override
	public String generate() {
		return EMPTY;
	}

}
