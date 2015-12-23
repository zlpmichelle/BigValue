package com.cloudera.bigdata.analysis.solution.iptv;

import java.io.IOException;

import java.text.DecimalFormat;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MinMaxPriorityQueue;

public class CubeReducer extends Reducer<Text, IPTVMetrics, Text, Text> {
	private static final Logger LOG = LoggerFactory.getLogger(CubeGenerator.class);
	private int totalProcessed = 0;
	private DecimalFormat format;
	private long recordNumThreshold = 0;
	private double failRatioThreshold = 0.0001;
	private MinMaxPriorityQueue<OutputMetrics> priorityQueue;
	private int topN;
	private long totalRecords;
	private long totalFailRecords;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

        totalRecords = Long.parseLong(conf.get("totalrecords"));
        totalFailRecords = Long.parseLong(conf.get("totalfailRecords"));
        LOG.info("totalrecords:"+totalRecords);
        LOG.info("totalFailRecords:"+totalFailRecords);
		String ratioStr = conf.get(CubeGenerator.FAIL_RATIO_THRESHOLD_KEY);
		recordNumThreshold = Long.parseLong(conf.get(CubeGenerator.RECORD_THESHOLD_KEY));
		failRatioThreshold = Double.parseDouble(ratioStr);
		//topN = Integer.parseInt(conf.get(CubeGenerator.TOP_FACTOR_NUMBER));
		
		int digitNum = ratioStr.substring(ratioStr.indexOf(".") + 1).length();
		digitNum += 4;
		StringBuilder sb = new StringBuilder();
		sb.append("#.");
		for(int i=0;i<digitNum;i++){
			sb.append("#");
		}
		
		format = new DecimalFormat(sb.toString());
		priorityQueue = MinMaxPriorityQueue.<OutputMetrics>orderedBy(new Comparator<OutputMetrics>() {

			@Override
			public int compare(OutputMetrics o1, OutputMetrics o2) {
				double qosWeight1 = (double)o1.getQosWeight();
				double qosWeight2 = (double)o2.getQosWeight();
				if(qosWeight1>qosWeight2) return 1;
				if(qosWeight1<qosWeight2) return -1;
				return 0;
			}
		}).<OutputMetrics>create();
	}
	
	protected void reduce(Text key, Iterable<IPTVMetrics> values, Context context) 
		throws IOException, InterruptedException {
		long totalReqNum = 0;
		long totalFailNum = 0;
		long totalRecordNum = 0;
		long totalFailRecordNum = 0;
		
		for(IPTVMetrics metrics : values){
			totalReqNum += metrics.getReqNum();
			if(metrics.getFailNum() != 0){
			totalFailRecordNum++;
			totalFailNum += metrics.getFailNum();
			}
			totalRecordNum ++;
		}
		
		totalProcessed += totalRecordNum;
		
		//LOG.info("@@@@@ The reducer already processed: " + totalProcessed);
		
		if(totalFailNum>0 && totalRecordNum >=recordNumThreshold){
			/*double qosWeight = (double)totalFailNum/(double)(totalReqNum * totalRecordNum);
			if(qosWeight>=failRatioThreshold){
				OutputMetrics aggregateMetrics = new OutputMetrics(new Text(key), totalReqNum, totalFailNum, totalRecordNum);
				priorityQueue.add(aggregateMetrics);
				//context.write(key, new Text(aggregateMetrics.toString() + "," + format.format(qosWeight)));
			}*/
			double qosWeight = (double)totalFailRecordNum/(double)totalFailRecords - (double)totalRecordNum/(double)totalRecords;
			if(qosWeight >= failRatioThreshold){
				OutputMetrics aggregateMetrics = new OutputMetrics(new Text(key), totalReqNum, totalFailNum, totalRecordNum,totalFailRecordNum,qosWeight);
				priorityQueue.add(aggregateMetrics);
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		while(!priorityQueue.isEmpty()){
			OutputMetrics metrics = priorityQueue.removeLast();
			double qosWeight = metrics.getQosWeight();
			context.write(metrics.getKey(), new Text(metrics.toString() + "," +metrics.getTotalFailNum()+","+format.format(qosWeight)));
		}
	}
	
}
