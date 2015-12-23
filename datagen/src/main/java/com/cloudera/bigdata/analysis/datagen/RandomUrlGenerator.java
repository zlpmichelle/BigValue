package com.cloudera.bigdata.analysis.datagen;

import java.util.Random;

import com.cloudera.bigdata.analysis.generated.ColumnSpec;

/**
 * URL generator can be specified as following patterns:
 * a. xxx.xxx.xx.xx
 * b. xxx.xxx.xx.xx:xx
 * c. xx-xxx.xxx.xxx.xxx:xx-xxx
 * d. *.xx.xx.xx:*
 */
public class RandomUrlGenerator extends FieldGenerator {
  String pattern = "";
  String col = "";
  String[] ipSegments = null;
  String[] fullAddr = null;
  int[] min = new int[4];
  int[] max = new int[4];
  int minPort = 1;
  int maxPort = 65536;
  Random rand = new Random();
  
  public RandomUrlGenerator(ColumnSpec columnSpec){
    this(null, columnSpec);
  }

  public RandomUrlGenerator(RecordGenerator recordGenerator, ColumnSpec columnSpec){
    super(recordGenerator, columnSpec);
    
    pattern = columnSpec.getValue();
    pattern = pattern.trim();
    fullAddr = pattern.split(":");
    ipSegments = fullAddr[0].split("\\.");
    
    for(int i=0;i<ipSegments.length;i++){
      String[] tokens =ipSegments[i].split("\\-");
      min[i] = 1;
      max[i] = 255;
      if(tokens.length==1 && !tokens[0].startsWith("*")){
        min[i] = Integer.valueOf(tokens[0]);
        max[i] = min[i];
      }
      if(tokens.length==2){
        min[i] = Integer.valueOf(tokens[0]);
        max[i] = Integer.valueOf(tokens[1]);
      }
    }
    
    // have the port part
    if(fullAddr.length==2){
      minPort = 1;
      maxPort = 65536;
      
      String[] tokens = fullAddr[1].split("\\-");
      if(tokens.length==1 && !tokens[0].startsWith("*")){
        minPort = Integer.valueOf(tokens[0]);
        maxPort = minPort;
      }
      if(tokens.length==2){
        minPort = Integer.valueOf(tokens[0]);
        maxPort = Integer.valueOf(tokens[1]);
      }
    }
  }
  
  @Override
  public String generate() {
	// The length of URL won't larger than 32.
	StringBuilder sb = new StringBuilder(32);
	
	for(int i=0;i<ipSegments.length;i++){
		if(min[i]==max[i]){
			sb.append(min[i]);
		}else{
			int value = random.nextInt(max[i]- min[i]) + min[i];
			sb.append(value);
		}
		
		if(i<ipSegments.length-1){
			sb.append(".");
		}
	}
	
	if(fullAddr.length == 2){
		sb.append(":");
		
		if(minPort == maxPort){
			sb.append(minPort);
		}else{
			int value = random.nextInt(maxPort - minPort) + minPort;
			sb.append(value);
		}
	}
	
	return sb.toString();
  }

}
