package com.cloudera.bigdata.analysis.solution.iptv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.cloudera.bigdata.analysis.core.Constants;
import com.cloudera.bigdata.analysis.util.Util;

public class CubeMapper extends Mapper<LongWritable, Text, Text, IPTVMetrics> {
	private final static int REQNUM_IDX = 2;
	private final static int FAILNUM_IDX = 3;
	private final static int ACCESSTYPE_IDX = 4;
	private final static int STBVERSION_IDX = 5;
	private final static int UPLINKID_IDX = 6;
	private final static int DIMENSIONS = 3;
	private final static int DIMENSIONS13 = 13;
	private List<List<Integer>> combinationList = new ArrayList<List<Integer>>();
	Random r = new Random();
	boolean simulate = false;
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		if(conf.get("simulate13").equals("true")){
			simulate = true;
		}
		int[] indexArray = null;
		int probs = 0;
		int maskbits = 0;
		if(simulate){
			indexArray = new int[] { 4,5,6,7,8,9,10,11,12,13,14,15,16};
			probs = (int) Math.pow(2, DIMENSIONS13);
			maskbits = DIMENSIONS13;
		}else{
			indexArray = new int[] {4,5,6};
			probs = (int) Math.pow(2, DIMENSIONS);
			maskbits = DIMENSIONS;
		}
		 
		for (int i = 1; i < probs; i++) {
			int[] masks = Util.getByteMasks(i, maskbits);
			List<Integer> combination = new ArrayList<Integer>();
			for (int j = 0; j < masks.length; j++) {
				if (masks[j] > 0) {
					combination.add(indexArray[j]);
				}
			}
			combinationList.add(combination);
		}
	}

	public void map(LongWritable key, Text text, Context context)
			throws IOException, InterruptedException {
		String line = text.toString();
		String[] fields = line.split(",");

		IPTVMetrics metrics = new IPTVMetrics();
		metrics.setReqNum(Long.parseLong(fields[REQNUM_IDX]));
		long l = Long.parseLong(fields[FAILNUM_IDX]);
		metrics.setFailNum(l);
		

		for (List<Integer> combination : combinationList) {
			StringBuilder sb = new StringBuilder();
			int combinationSize = combination.size();
			for (int i = 0; i < combination.size(); i++) {
				int a = combination.get(i);
				if(simulate){
				if(a == 4){
					sb.append("0000000"+r.nextInt(10));
				}
				if(a == 5){
					sb.append("1000000"+r.nextInt(10));
				}
				if(a == 6){
					sb.append("0002200"+r.nextInt(10));
				}
				if(a == 7){
					sb.append("0500000"+r.nextInt(10));
				}
				if(a == 8){
					sb.append("0000007"+r.nextInt(10));
				}
				if(a == 9){
					sb.append("00058700"+r.nextInt(10));
				}
				if(a == 10){
					sb.append("03450000"+r.nextInt(10));
				}
				if(a == 11){
					sb.append("00001110"+r.nextInt(10));
				}
				if(a == 12){
					sb.append("0067000"+r.nextInt(10));
				}
				if(a == 13){
					sb.append("7000000"+r.nextInt(10));
				}
				if(a == 14){
					sb.append("0009810"+r.nextInt(10));
				}
				if(a == 15){
					sb.append("0029500"+r.nextInt(10));
				}
				if(a == 16){
					sb.append("00124500"+r.nextInt(10));
				}
				}else{
					sb.append(fields[a]);
				}
				if (i < combinationSize - 1) {
					sb.append(Constants.COMMA_SEP);
				}
			}

	context.write(new Text(sb.toString()), metrics);	
		}
	}
}
