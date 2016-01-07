package com.cloudera.bigdata.analysis.solution.y;

import org.apache.hadoop.io.Text;

public class OutputMetrics extends YMetrics {
	private long totalFailNum;
	private double QosWeight;
	private Text key;
	
	public OutputMetrics(Text key) {
		this.key = key;
	}
	
	public OutputMetrics(Text key, long reqNum, long failNum, long recordNum,long totalFailRecordNum,double Qos){
		super(reqNum, failNum, recordNum);
		this.key = key;
		this.totalFailNum = totalFailRecordNum;
		this.QosWeight = Qos;
	}
	
	public Text getKey(){
		String[] s = new String(key.getBytes()).split(",");
		StringBuilder sb =new StringBuilder();
		for(int i =0;i<s.length-1;i++){
			sb.append(s[i]+"#");
		}
		sb.append(s[s.length-1]+",");
		return new Text(sb.toString());
	}
	public long getTotalFailNum(){
		return totalFailNum;
	}
	public double getQosWeight(){
		return QosWeight;
	}
}
