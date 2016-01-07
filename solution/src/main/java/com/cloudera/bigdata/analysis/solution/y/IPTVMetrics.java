package com.cloudera.bigdata.analysis.solution.y;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class YMetrics implements WritableComparable<YMetrics>{
	
	private long reqNum;
	private long failNum;
	private long recordNum;
	
	public YMetrics(){
		this(0, 0);
	}
	
	public YMetrics(long reqNum, long failNum){
		this(reqNum, failNum, 1);
	}
	
	public YMetrics(long reqNum, long failNum, long recordNum){
		this.reqNum = reqNum;
		this.failNum = failNum;
		this.recordNum = recordNum;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(reqNum + "," + failNum + "," + recordNum);
		
		return sb.toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		reqNum = dataInput.readLong();
		failNum = dataInput.readLong();
		recordNum = dataInput.readLong();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(reqNum);
		dataOutput.writeLong(failNum);
		dataOutput.writeLong(recordNum);
	}

	@Override
	public int compareTo(YMetrics o) {
		// TODO Auto-generated method stub
		YMetrics thisMetrics = this;
		YMetrics thatMetrics = (YMetrics)o;
		
		return 0;
	}

	public long getReqNum() {
		return reqNum;
	}

	public void setReqNum(long reqNum) {
		this.reqNum = reqNum;
	}

	public long getFailNum() {
		return failNum;
	}

	public void setFailNum(long failNum) {
		this.failNum = failNum;
	}

	public long getRecordNum() {
		return recordNum;
	}

	public void setRecordNum(long recordNum) {
		this.recordNum = recordNum;
	}

}
