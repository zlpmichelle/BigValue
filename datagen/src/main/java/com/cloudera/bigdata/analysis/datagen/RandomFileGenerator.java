package com.cloudera.bigdata.analysis.datagen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

public class RandomFileGenerator extends FieldGenerator {
	
	private String fileName;
	private List<String> list;
	private int size;
	
	private Random random = new Random();

	public RandomFileGenerator(ColumnSpec columnSpec) {
		this(null, columnSpec);
	}
	
	public RandomFileGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec) {
		super(recordGenerator, columnSpec);
		
		
		fileName = columnSpec.getValue();
		
		list = new ArrayList<String>();
		BufferedReader reader = null;
		try{
  		reader = new BufferedReader(new FileReader(fileName));
  		String line = null;
  		while((line = reader.readLine())!= null){
  			if(line.startsWith("#")){
  				continue;
  			}else{
  				list.add(line);
  			}
  		}
		}catch (IOException e) {
      
    }finally{
      if(reader!=null){
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
		
		size = list.size();
	}
	
	@Override
	public String generate() {
		return list.get(random.nextInt(size));
	}

}
