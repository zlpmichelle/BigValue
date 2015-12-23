package com.cloudera.bigdata.analysis.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Index;
import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto.RecordService;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto.RefreshRequest;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto.RefreshResponse;
import com.cloudera.bigdata.analysis.index.search.SearchStrategy;
import com.cloudera.bigdata.analysis.index.search.SearchStrategyDecider;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class RecordServiceEndpoint extends RecordService implements
    Coprocessor, CoprocessorService {
  private RegionCoprocessorEnvironment env;
  private Configuration conf;
  private static boolean licenseStatus;

  private static final Logger LOG = LoggerFactory
      .getLogger(RecordServiceEndpoint.class);
  private HRegion region;

  private SearchStrategyDecider searchStrategyDecider;

  @Override
  public void query(RpcController controller, QueryTypeProto.Query request,
      RpcCallback<QueryTypeProto.QueryResult> done) {
    Query query = ProtoUtil.toQuery(request);
    licenseStatus = true;
    if (licenseStatus) {
      LOG.info("Receive query request on region: ["
          + region.getRegionNameAsString() + "]: " + request.toString());
      SearchStrategy searchStrategy = searchStrategyDecider.decide(query);
      QueryResult recordQueryResult = searchStrategy.doSearch(region, query);
      LOG.debug("Result:" + recordQueryResult.proto.getRecordsCount());
      done.run(recordQueryResult.proto);
    } else {
      LOG.error("License Invalidation");
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
      region = this.env.getRegion();
      conf = this.env.getConfiguration();
      searchStrategyDecider = new SearchStrategyDecider();
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }

  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // Nothing to do

  }

  /*-----------------------------------   Endpoint (QueryService) Methods   ----------------------------------------*/
  @Override
  public void refreshIndexMap(RpcController controller, RefreshRequest request,
      RpcCallback<RefreshResponse> done) {
    String tableName = ProtoUtil.toRefreshRequest(request);
    // refresh indexMap
    Index.newIndexMap(tableName);
    RecordServiceProto.RefreshResponse refreshResult = Index
        .refreshIndexMap(tableName);
    done.run(refreshResult);
  }
}
