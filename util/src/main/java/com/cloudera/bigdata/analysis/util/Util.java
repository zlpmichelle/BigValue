package com.cloudera.bigdata.analysis.util;

import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;

import com.cloudera.bigdata.analysis.core.Constants;


public class Util {
  private final static String OS_ENV = "os.name";
	private final static int[] EMPTY_INTS = new int[]{};
	
	public static int[] getByteMasks(int val, int size) {
		if(val<0){
			return EMPTY_INTS;
		}
		
		int[] result = new int[size];
		for(int i=0;i<size;i++){
			int current = val % 2;
			result[i] = current;
			val /= 2;
		}
		
		return result;
	}
	
	public static String formatDouble(DecimalFormat format, double val){
		return format.format(val);
	}
	
	public static void setNumOfReduceTasks(Configuration conf, int tasks){
		conf.set(Constants.REDUCE_TASKS_KEY, String.valueOf(tasks));
	}
	
	public static void setProperty(Configuration conf, String key, String value){
		conf.set(key, value);
	}
	
	public static void testGetByteMasks(){
		int[] masks = Util.getByteMasks(2, 3);
		for(int i =0;i<masks.length;i++){
			System.out.print(masks[i] + ",");
		}
	}
	
	public static void main(String[] args){
		Util.testGetByteMasks();
	}
	
	public static boolean isWindows(){
	  String osName = System.getProperty(OS_ENV).toLowerCase();
	  return osName.indexOf("win") >= 0;
	}
	
	// Explicitly set the RawLocalFileSystem will disable crc checksum
	public static void configLocalFileImpl(Configuration conf){
	  conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
	}
}
