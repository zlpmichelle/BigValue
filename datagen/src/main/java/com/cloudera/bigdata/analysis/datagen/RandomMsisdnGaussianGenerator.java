package com.cloudera.bigdata.analysis.datagen;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

/**
 * The generation pattern can be specified as:
 * <startPhoneNum>,<endPhoneNum>
 */
public class RandomMsisdnGaussianGenerator extends FieldGenerator {
	
	private static final Logger LOG = LoggerFactory.getLogger(RandomMsisdnGaussianGenerator.class);
	
	private long min;
	private long max;
	
	private Random rand = new Random();
	
	public RandomMsisdnGaussianGenerator(ColumnSpec columnSpec){
		this(null, columnSpec);
	}
	
	public RandomMsisdnGaussianGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
		super(recordGenerator, columnSpec);
		
		try{
  		min = Long.valueOf(columnSpec.getStart());
  		max = Long.valueOf(columnSpec.getEnd());
  		
  		if(min>max){
  		  max = min;
  		}
		}catch (NumberFormatException e) {
      LOG.error("Format exception " + this, e);
    }
	}

	@Override
	public String generate() {
		double gauVal = Math.abs(rand.nextGaussian() / 3.0 + 0.5);
		String tmpval = String.valueOf((gauVal * (max - min) + min));
		return Md5Util.getMD5Str(tmpval);
	}

}
