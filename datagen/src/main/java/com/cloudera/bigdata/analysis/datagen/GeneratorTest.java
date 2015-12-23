package com.cloudera.bigdata.analysis.datagen;

import java.util.Timer;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class GeneratorTest {
	
	public static void main(String[] args){
		
		
		//testRandomUrlGenerator();
		
		//testRandomGaussianGenerator();
		
		//Timer timer;
	  long start = System.currentTimeMillis();
	  for(int i = 0;i<10000000;i++){
	    System.currentTimeMillis();
	  }
	  System.out.println(System.currentTimeMillis() - start);
	}
	
	public static void testRandomUrlGenerator(){
	  ColumnSpec columnSpec = new ColumnSpec();
	  columnSpec.setValue("192-210.168-235.*.*:*");
		RandomUrlGenerator generator = new RandomUrlGenerator(columnSpec);
		
		System.out.println(generator.generate());
	}
	
	public static void testRandomGaussianGenerator(){
	  ColumnSpec columnSpec = new ColumnSpec();
	  columnSpec.setStart("13306780893");
	  columnSpec.setEnd("13456678796");
		RandomMsisdnGaussianGenerator generator = 
			new RandomMsisdnGaussianGenerator(columnSpec);
		
		System.out.println(generator.generate());
	}
}
