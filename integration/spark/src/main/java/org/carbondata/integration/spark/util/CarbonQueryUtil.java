/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.integration.spark.partition.api.Partition;
import org.carbondata.integration.spark.partition.api.impl.DefaultLoadBalancer;
import org.carbondata.integration.spark.partition.api.impl.PartitionMultiFileImpl;
import org.carbondata.integration.spark.partition.api.impl.QueryPartitionHelper;
import org.carbondata.integration.spark.splits.TableSplit;
import org.carbondata.query.carbon.model.CarbonQueryPlan;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.directinterface.impl.CarbonQueryParseUtil;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.scope.QueryScopeObject;

import org.apache.spark.sql.cubemodel.Partitioner;
/**
 * This utilty parses the Carbon query plan to actual query model object.
 */
public final class CarbonQueryUtil {

  private CarbonQueryUtil() {

  }

  /**
   * API will provide the slices
   *
   * @param executerModel
   * @param basePath
   * @param partitionID
   * @return
   */
  public static List<String> getSliceLoads(CarbonQueryExecutorModel executerModel, String basePath,
      String partitionID) {

    List<String> listOfLoadPaths = new ArrayList<String>(20);
    if (null != executerModel) {
      List<String> listOfLoad = executerModel.getListValidSliceNumbers();

      if (null != listOfLoad) {
        for (String name : listOfLoad) {
          String loadPath = CarbonCommonConstants.LOAD_FOLDER + name;
          listOfLoadPaths.add(loadPath);
        }
      }
    }

    return listOfLoadPaths;

  }

  /**
   * It creates the one split for each region server.
   */
  public static synchronized TableSplit[] getTableSplits(String schemaName, String cubeName,
      CarbonQueryPlan queryPlan, Partitioner partitioner) throws IOException {

    //Just create splits depends on locations of region servers
    List<Partition> allPartitions = null;
    if (queryPlan == null) {
      allPartitions =
          QueryPartitionHelper.getInstance().getAllPartitions(schemaName, cubeName, partitioner);
    } else {
      allPartitions =
          QueryPartitionHelper.getInstance().getPartitionsForQuery(queryPlan, partitioner);
    }
    TableSplit[] splits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = QueryPartitionHelper.getInstance()
          .getLocation(partition, schemaName, cubeName, partitioner);
      locations.add(location);
      splits[i].setPartition(partition);
      splits[i].setLocations(locations);
    }

    return splits;
  }

  /**
   * It creates the one split for each region server.
   */
  public static TableSplit[] getTableSplitsForDirectLoad(String sourcePath, String[] nodeList,
      int partitionCount) throws Exception {

    //Just create splits depends on locations of region servers
    FileType fileType = FileFactory.getFileType(sourcePath);
    DefaultLoadBalancer loadBalancer = null;
    List<Partition> allPartitions = getAllFilesForDataLoad(sourcePath, fileType, partitionCount);
    loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
    TableSplit[] tblSplits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < tblSplits.length; i++) {
      tblSplits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = loadBalancer.getNodeForPartitions(partition);
      locations.add(location);
      tblSplits[i].setPartition(partition);
      tblSplits[i].setLocations(locations);
    }
    return tblSplits;
  }

  /**
   * It creates the one split for each region server.
   */
  public static TableSplit[] getPartitionSplits(String sourcePath, String[] nodeList,
      int partitionCount) throws Exception {

    //Just create splits depends on locations of region servers
    FileType fileType = FileFactory.getFileType(sourcePath);
    DefaultLoadBalancer loadBalancer = null;
    List<Partition> allPartitions = getAllPartitions(sourcePath, fileType, partitionCount);
    loadBalancer = new DefaultLoadBalancer(Arrays.asList(nodeList), allPartitions);
    TableSplit[] splits = new TableSplit[allPartitions.size()];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new TableSplit();
      List<String> locations = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      Partition partition = allPartitions.get(i);
      String location = loadBalancer.getNodeForPartitions(partition);
      locations.add(location);
      splits[i].setPartition(partition);
      splits[i].setLocations(locations);
    }
    return splits;
  }

  public static void getAllFiles(String sourcePath, List<String> partitionsFiles, FileType fileType)
      throws Exception {

    if (!FileFactory.isFileExist(sourcePath, fileType, false)) {
      throw new Exception("Source file doesn't exist at path: " + sourcePath);
    }

    CarbonFile file = FileFactory.getCarbonFile(sourcePath, fileType);
    if (file.isDirectory()) {
      CarbonFile[] fileNames = file.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile pathname) {
          return true;
        }
      });
      for (int i = 0; i < fileNames.length; i++) {
        getAllFiles(fileNames[i].getPath(), partitionsFiles, fileType);
      }
    } else {
      // add only csv files
      if (file.getName().endsWith("csv")) {
        partitionsFiles.add(file.getPath());
      }
    }
  }

  private static List<Partition> getAllFilesForDataLoad(String sourcePath, FileType fileType,
      int partitionCount) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    getAllFiles(sourcePath, files, fileType);
    List<Partition> partitionList =
        new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Map<Integer, List<String>> partitionFiles = new HashMap<Integer, List<String>>();

    for (int i = 0; i < partitionCount; i++) {
      partitionFiles.put(i, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN));
      partitionList.add(new PartitionMultiFileImpl(i + "", partitionFiles.get(i)));
    }
    for (int i = 0; i < files.size(); i++) {
      partitionFiles.get(i % partitionCount).add(files.get(i));
    }
    return partitionList;
  }

  private static List<Partition> getAllPartitions(String sourcePath, FileType fileType,
      int partitionCount) throws Exception {
    List<String> files = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    getAllFiles(sourcePath, files, fileType);
    int[] numberOfFilesPerPartition = getNumberOfFilesPerPartition(files.size(), partitionCount);
    int startIndex = 0;
    int endIndex = 0;
    List<Partition> partitionList =
        new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    if (numberOfFilesPerPartition != null) {
      for (int i = 0; i < numberOfFilesPerPartition.length; i++) {
        List<String> partitionFiles =
            new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        endIndex += numberOfFilesPerPartition[i];
        for (int j = startIndex; j < endIndex; j++) {
          partitionFiles.add(files.get(j));
        }
        startIndex += numberOfFilesPerPartition[i];
        partitionList.add(new PartitionMultiFileImpl(i + "", partitionFiles));
      }
    }
    return partitionList;
  }

  private static int[] getNumberOfFilesPerPartition(int numberOfFiles, int partitionCount) {
    int div = numberOfFiles / partitionCount;
    int mod = numberOfFiles % partitionCount;
    int[] numberOfNodeToScan = null;
    if (div > 0) {
      numberOfNodeToScan = new int[partitionCount];
      Arrays.fill(numberOfNodeToScan, div);
    } else if (mod > 0) {
      numberOfNodeToScan = new int[mod];
    }
    for (int i = 0; i < mod; i++) {
      numberOfNodeToScan[i] = numberOfNodeToScan[i] + 1;
    }
    return numberOfNodeToScan;
  }

  public static QueryScopeObject createDataSource(int currentRestructNumber, Schema schema,
      Cube cube, String partitionID, List<String> sliceLoadPaths, String factTableName,
      String basePath, long cubeCreationTime, LoadMetadataDetails[] loadMetadataDetails) {
    QueryScopeObject queryScopeObject = InMemoryTableStore.getInstance()
        .loadCube(schema, cube, partitionID, sliceLoadPaths, factTableName, basePath,
            currentRestructNumber, cubeCreationTime, loadMetadataDetails);
    return queryScopeObject;
  }

  public static boolean isQuickFilter(QueryModel queryModel) {
    return ("true".equals(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_ENABLE_QUICK_FILTER)) && null == queryModel
        .getFilterExpressionResolverTree() && queryModel.getQueryDimension().size() == 1
        && queryModel.getQueryMeasures().size() == 0
        && queryModel.getDimAggregationInfo().size() == 0 && queryModel.getExpressions().size() == 0
        && !queryModel.isDetailQuery());
  }

  public static List<String> getListOfSlices(LoadMetadataDetails[] details) {
    List<String> slices = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    if (null != details) {
      for (LoadMetadataDetails oneLoad : details) {
        if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(oneLoad.getLoadStatus())) {
          String loadName = CarbonCommonConstants.LOAD_FOLDER + oneLoad.getLoadName();
          slices.add(loadName);
        }
      }
    }
    return slices;
  }

  public static void clearLevelLRUCacheForDroppedColumns(List<String> listOfLoadFolders,
      List<String> columns, String schemaName, String cubeName, int partitionCount) {
    CarbonQueryParseUtil
        .removeDroppedColumnsFromLevelLRUCache(listOfLoadFolders, columns, schemaName, cubeName,
            partitionCount);
  }
}
