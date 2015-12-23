package com.cloudera.bigdata.analysis.datagen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.datagen.GeneratorDriver.ParameterSet;


public class FastCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(FastCalculator.class);
  private static final int FIXED_THD_NUM = 8;
  private ExecutorService executor;
  private ParameterSet parameterSet;
  private long total;
  private long minValue;
  private long maxValue;
  private long accumulator;
  private List<Long> valueList;
  
  public FastCalculator(ParameterSet parameterSet){
    this.parameterSet = parameterSet;
    this.total = parameterSet.totalNum;
    
    //added by jemmy
    if (parameterSet.useSizeMeasurement) {
    	this.minValue = parameterSet.minSize;
    	this.maxValue = parameterSet.maxSize;
    } else {
    	this.minValue = parameterSet.minNum;
    	this.maxValue = parameterSet.maxNum;
    }
    //ended add
    
//    this.minValue = parameterSet.minSize;
//    this.maxValue = parameterSet.maxSize;
    
    if(parameterSet.useSizeMeasurement){
      this.total = parameterSet.totalSize * 1024;
      this.minValue = parameterSet.minSize;
      this.maxValue = parameterSet.maxSize;
    }
    executor = Executors.newFixedThreadPool(FIXED_THD_NUM);
    accumulator = 0;
    valueList = new ArrayList<Long>();
    
    for(int i=0;i<FIXED_THD_NUM;i++){
      executor.submit(new CalculatorThead());
    }
    
    executor.shutdown();
  }
  
  public void awaitFinish(){
    while(!executor.isTerminated()){
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
   
  }
  
  /*
   * With specified slots, the aggregated sum in each slot varies the least
   */
  public List<List<Long>> group(){
    int slotNum = parameterSet.parallelism;
    PriorityQueue<AutoSumArrayList> minHeap = new PriorityQueue<AutoSumArrayList>(slotNum);
    for(long i = 0;i<slotNum;i++){
      AutoSumArrayList slot = new AutoSumArrayList();
      minHeap.add(slot);
    }
    
    Collections.sort(valueList);
    for(int j=valueList.size()-1;j>=0;j--){
      // actually we only need to peek one and add value to it
      // then adjust the heap
      AutoSumArrayList list = minHeap.poll();
      list.add(valueList.get(j));
      minHeap.offer(list);
    }
    
    if(parameterSet.debug){
      Iterator<AutoSumArrayList> iter = minHeap.iterator();
      while(iter.hasNext()){
        AutoSumArrayList list = iter.next();
        System.out.print(list.getSum() + ":");
        for(int i=0;i<list.size();i++){
          System.out.print(list.get(i) + ",");
        }
        System.out.println();
      }
    }
    
    return trim(minHeap.toArray(new AutoSumArrayList[]{}));
  }
  
  private List<List<Long>> trim(List<Long>[] array){
      List<List<Long>> list = new ArrayList<List<Long>>();
      for(int i=0;i<array.length;i++){
        if(array[i].size()>0){
          list.add(array[i]);
        }
      }
      
      return list;
  }
  
  public synchronized boolean accumulate(long value){
    if(accumulator + value <= total){
      accumulator += value;
      valueList.add(value);
      return true;
    }else{
      LOG.debug("Accumulated with " + accumulator);
      return false;
    }
  }
  
  private class CalculatorThead implements Runnable {
    private Random random;
    public CalculatorThead(){
      random = new Random();
    }
    
    @Override
    public void run() {
      while(true){
        long value = (maxValue==minValue)?maxValue:(Math.abs(random.nextLong())%(maxValue - minValue)
            + minValue);
        
        if(!accumulate(value)){
          break;
        }
      }
    }
  }
  
  @SuppressWarnings("serial")
  private static class AutoSumArrayList extends ArrayList<Long> implements Comparable<AutoSumArrayList>{
    
    private Long sum = 0L;
    
    public boolean add(Long e){
      if(super.add(e)){
        sum += e;
        return true;
      }else{
        return false;
      }
    }
    
    public long getSum(){
      return sum;
    }

    @Override
    public int compareTo(AutoSumArrayList o) {
      return sum.compareTo(o.sum);
    }
  }
}
