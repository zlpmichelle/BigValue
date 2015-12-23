package com.cloudera.bigdata.analysis.index.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto.RefreshRequest;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtoUtil {

  public static Logger LOG = LoggerFactory.getLogger(ProtoUtil.class.getName());

  public static QueryTypeProto.Query toQueryProto(Query query) {
    QueryTypeProto.Query.Builder builder = QueryTypeProto.Query.newBuilder();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream dos;
    try {
      dos = new DataOutputStream(bas);
      query.write(dos);
      builder.setData(HBaseZeroCopyByteString.wrap(bas.toByteArray()));
      dos.close();
      bas.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return builder.build();
  }

  public static Query toQuery(QueryTypeProto.Query proto) {
    try {
      Query query = new Query();
      ByteArrayInputStream bis = new ByteArrayInputStream(proto.getData()
          .toByteArray());
      DataInputStream dis;
      dis = new DataInputStream(bis);
      query.readFields(dis);
      dis.close();
      bis.close();
      return query;
    } catch (InvalidProtocolBufferException e) {
      LOG.error(e.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

  /*
   * public static QueryTypeProto.Query toQueryProto(Query query){
   * QueryTypeProto.Query.Builder builder = QueryTypeProto.Query.newBuilder();
   * Condition condition = query.getCondition();
   * 
   * if(condition instanceof SimpleCondition){
   * builder.setCondition(toSimpleConditionProto((SimpleCondition)condition)); }
   * else if(condition instanceof CompositeCondition){
   * QueryTypeProto.Condition.Builder conditionBuilder =
   * QueryTypeProto.Condition.newBuilder(); List<QueryTypeProto.Condition>
   * conditionList = new ArrayList<QueryTypeProto.Condition>();
   * addChildConditionProto(conditionList, condition);
   * conditionBuilder.addAllConditons(conditionList);
   * builder.setCondition(conditionBuilder.build()); } return builder.build(); }
   * 
   * private static QueryTypeProto.Condition
   * toSimpleConditionProto(SimpleCondition sc){
   * QueryTypeProto.Condition.Builder conditionBuilder =
   * QueryTypeProto.Condition.newBuilder();
   * conditionBuilder.setClassName(sc.getClass().getName());
   * conditionBuilder.setColumnFamily
   * (HBaseZeroCopyByteString.wrap(sc.getColumnFamily().getBytes()));
   * conditionBuilder
   * .setOperator(QueryTypeProto.Operator.valueOf(sc.getOperator().toString()));
   * conditionBuilder
   * .setQualifier(HBaseZeroCopyByteString.wrap(sc.getQualifier().getBytes()));
   * conditionBuilder
   * .setValue(HBaseZeroCopyByteString.wrap(sc.getValue().getBytes())); return
   * conditionBuilder.build(); }
   * 
   * private static void addChildConditionProto(List<QueryTypeProto.Condition>
   * conditionList, Condition condition){ if(condition instanceof
   * SimpleCondition){
   * conditionList.add(toSimpleConditionProto((SimpleCondition)condition)); }
   * else if(condition instanceof CompositeCondition){ CompositeCondition cd =
   * (CompositeCondition)condition; for(Condition c:cd.getChildren()){
   * addChildConditionProto(conditionList, c); } } }
   * 
   * 
   * private static SimpleCondition toSimpleCondition(QueryTypeProto.Condition
   * cp) { SimpleCondition c=null; try { Class<?> clazz =
   * Class.forName(cp.getClassName()); c = (SimpleCondition)
   * clazz.newInstance(); c.setColumnFamily(new
   * ByteArray(cp.getColumnFamily().toByteArray()));
   * c.setOperator(Operator.valueOf(cp.getOperator().name()));
   * c.setQualifier(new ByteArray(cp.getQualifier().toByteArray()));
   * c.setValue(new ByteArray(cp.getValue().toByteArray()));
   * 
   * } catch (ClassNotFoundException e) { LOG.error(e.getMessage()); } catch
   * (InstantiationException e) { LOG.error(e.getMessage()); } catch
   * (IllegalAccessException e) { LOG.error(e.getMessage()); } return c; }
   * 
   * 
   * private static void addChildCondition(List<Condition> conditionList,
   * QueryTypeProto.Condition cp){
   * if(cp.getClassName().equals(SimpleCondition.class.getName())){
   * conditionList.add(toSimpleCondition(cp)); } else {
   * for(QueryTypeProto.Condition c:cp.getConditonsList()){
   * addChildCondition(conditionList, c); } } }
   * 
   * 
   * public static Query toQuery(QueryTypeProto.Query qp){ Query query = new
   * Query(); query.setOrder(Order.valueOf(qp.getOrder().name()));
   * query.setOrderByColumnFamily(new
   * ByteArray(qp.getOrderByColumnFamily().toByteArray()));
   * query.setOrderByQualifier(new
   * ByteArray(qp.getOrderByQualifier().toByteArray()));
   * query.setOrdered(qp.getOrderd());
   * query.setResultsLimit(qp.getResultsLimit()); query.setTable(new
   * ByteArray(qp.getTable().toByteArray()));
   * if(qp.getCondition().getClassName()
   * .equals(SimpleCondition.class.getName())){
   * query.setCondition(toSimpleCondition(qp.getCondition())); } else{
   * List<Condition> cl = new ArrayList<Condition>();
   * for(QueryTypeProto.Condition cp:qp.getCondition().getConditonsList()){
   * addChildCondition(cl, cp); } try { Class<?> clazz =
   * Class.forName(qp.getCondition().getClassName()); CompositeCondition c =
   * (CompositeCondition) clazz.newInstance(); for(Condition cd:cl){ c.add(cd);
   * } query.setCondition(c);
   * 
   * } catch (ClassNotFoundException e) { LOG.error(e.getMessage()); } catch
   * (InstantiationException e) { LOG.error(e.getMessage()); } catch
   * (IllegalAccessException e) { LOG.error(e.getMessage()); }
   * 
   * }
   * 
   * return query; }
   */

  public static QueryTypeProto.QueryResult toQueryResultProto(QueryResult qs) {
    return qs.proto;
  }

  public static QueryResult toQueryResult(QueryTypeProto.QueryResult qsp) {
    QueryResult qs = new QueryResult();
    qs.setProto(qsp);
    return qs;
  }

  public static RefreshRequest toRefreshRequestProto(String tableName) {
    RecordServiceProto.RefreshRequest.Builder builder = RecordServiceProto.RefreshRequest
        .newBuilder();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream dos;
    try {
      dos = new DataOutputStream(bas);
      Bytes.writeByteArray(dos, IndexUtil.convertStringToByteArray(tableName)
          .getBytes());
      builder.setTableName(HBaseZeroCopyByteString.wrap(bas.toByteArray()));
      dos.close();
      bas.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return builder.build();
  }

  public static String toRefreshRequest(RefreshRequest request) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(request
          .getTableName().toByteArray());
      DataInputStream dis;
      dis = new DataInputStream(bis);
      String tableName = IndexUtil.convertByteArrayToString(new ByteArray(Bytes
          .readByteArray(dis)).getBytes());
      dis.close();
      bis.close();
      return tableName;
    } catch (InvalidProtocolBufferException e) {
      LOG.error(e.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }
}