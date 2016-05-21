package org.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.QueryExecutor;
import org.carbondata.query.carbon.executor.QueryExecutorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;

/**
 */
public class CarbonCompactionExecutor {

  private QueryExecutor queryExecutor;
  private final SegmentProperties segmentProperties;
  private final String schemaName;
  private final String factTableName;
  private final Map<String, Map<String, List<TableBlockInfo>>> segmentMapping;
  private final String storePath;
  private CarbonTable carbonTable;

  public CarbonCompactionExecutor(Map<String, Map<String, List<TableBlockInfo>>> segmentMapping,
      SegmentProperties segmentProperties, String schemaName, String factTableName,
      String storePath, CarbonTable carbonTable) {

    this.segmentMapping = segmentMapping;

    this.segmentProperties = segmentProperties;

    this.schemaName = schemaName;

    this.factTableName = factTableName;

    this.storePath = storePath;

    this.carbonTable = carbonTable;
  }

  public List<CarbonIterator<BatchRawResult>> processTableBlocks() {

    List<CarbonIterator<BatchRawResult>> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // iterate each seg ID
    for (Map.Entry<String, Map<String, List<TableBlockInfo>>> taskMap : segmentMapping.entrySet()) {
      // for each segment
      Map<String, List<TableBlockInfo>> taskBlockListMapping = taskMap.getValue();

      for (Map.Entry<String, List<TableBlockInfo>> blockList : taskBlockListMapping.entrySet()) {

        List<TableBlockInfo> list = blockList.getValue();
        Collections.sort(list);
        resultList.add(executeBlockList(list));

      }
    }

    return resultList;
  }

  public CarbonIterator<BatchRawResult> executeBlockList(List<TableBlockInfo> blockList) {

    QueryModel model = prepareQueryModel(blockList);

    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(model);
    CarbonIterator<BatchRawResult> iter = null;
    try {
      iter = queryExecutor.execute(model);
    } catch (QueryExecutionException e) {
      e = null;
    }

    return iter;
  }

  public QueryModel prepareQueryModel(List<TableBlockInfo> blockList) {

    QueryModel model = new QueryModel();

    model.setTableBlockInfos(blockList);
    model.setCountStarQuery(false);
    model.setDetailQuery(true);
    model.setForcedDetailRawQuery(true);
    model.setFilterExpressionResolverTree(null);

    List<QueryDimension> dims = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (CarbonDimension dim : segmentProperties.getDimensions()) {
      QueryDimension queryDimension = new QueryDimension(dim.getColName());
      dims.add(queryDimension);
    }
    model.setQueryDimension(dims);

    List<QueryMeasure> msrs = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (CarbonMeasure carbonMeasure : segmentProperties.getMeasures()) {
      QueryMeasure queryMeasure = new QueryMeasure(carbonMeasure.getColName());
      msrs.add(queryMeasure);
    }
    model.setQueryMeasures(msrs);

    model.setQueryId(System.nanoTime() + "");

    model.setAbsoluteTableIdentifier(new AbsoluteTableIdentifier(storePath,
        new CarbonTableIdentifier(schemaName, factTableName)));

    model.setAggTable(false);
    model.setLimit(-1);

    model.setTable(carbonTable);

    return model;
  }

}
