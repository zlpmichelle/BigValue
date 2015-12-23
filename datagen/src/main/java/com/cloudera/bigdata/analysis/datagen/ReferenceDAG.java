package com.cloudera.bigdata.analysis.datagen;

import java.util.List;

public class ReferenceDAG {
  public ReferenceDAG(){
    DAGNode node = new DAGNode();
    
    node.getSelf();
    
    node.getDependencies();
  }
  
  private class DAGNode {
    private FieldGenerator generator;
    private List<FieldGenerator> dependencies;
    
    public FieldGenerator getSelf(){
      return generator;
    }
    
    public List<FieldGenerator> getDependencies(){
      return dependencies;
    }
  }
}
