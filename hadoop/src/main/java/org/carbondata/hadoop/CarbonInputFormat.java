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
package org.carbondata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.SegmentTaskIndexStore;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filters.FilterExpressionProcessor;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.StringUtils;

/**
 * Carbon Input format class representing one carbon table
 */
public class CarbonInputFormat<T> extends FileInputFormat<Void, T> {

  private static final Log LOG = LogFactory.getLog(CarbonInputFormat.class);

  private static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databasename";
  private static final String TABLE_NAME = "mapreduce.input.carboninputformat.tablename";
  //comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";

  public static void setTableToAccess(Job job, CarbonTableIdentifier tableIdentifier) {
    job.getConfiguration().set(CarbonInputFormat.DATABASE_NAME, tableIdentifier.getDatabaseName());
    job.getConfiguration().set(CarbonInputFormat.TABLE_NAME, tableIdentifier.getTableName());
  }

  /**
   * Set List of segments to access
   */
  public static void setSegmentsToAccess(Job job, List<Integer> segmentNosList) {

    //serialize to comma separated string
    StringBuilder stringSegmentsBuilder = new StringBuilder();
    for (int i = 0; i < segmentNosList.size(); i++) {
      Integer segmentNo = segmentNosList.get(i);
      stringSegmentsBuilder.append(segmentNo);
      if (i < segmentNosList.size() - 1) {
        stringSegmentsBuilder.append(",");
      }
    }
    job.getConfiguration()
        .set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, stringSegmentsBuilder.toString());
  }

  /**
   * Get CarbonTableIdentifier from job configuration
   */
  public static CarbonTableIdentifier getTableToAccess(JobContext job) {
    String databaseName = job.getConfiguration().get(CarbonInputFormat.DATABASE_NAME);
    String tableName = job.getConfiguration().get(CarbonInputFormat.TABLE_NAME);
    if (databaseName != null && tableName != null) {
      return new CarbonTableIdentifier(databaseName, tableName);
    }
    //TODO: better raise exception
    return null;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR
   * are used to get table path to read.
   *
   * @param job
   * @return List<InputSplit> list of CarbonInputSplit
   * @throws IOException
   */
  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    List<InputSplit> carbonSplits = new ArrayList<InputSplit>(splits.size());
    // identify table blocks
    for (InputSplit inputSplit : splits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      String segmentId = CarbonTablePath.DataPathUtil.getSegmentId(fileSplit.getPath().toString());
      if (CarbonCommonConstants.INVALID_SEGMENT_ID.equalsIgnoreCase(segmentId)) {
        continue;
      }
      carbonSplits.add(CarbonInputSplit.from(segmentId, fileSplit));
    }
    return carbonSplits;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job, Expression filterExpression)
      throws IOException, IndexBuilderException, QueryExecutionException {

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(getStorePathString(job), getTableToAccess(job));

    //get filter resolver
    FilterResolverIntf filterResolver =
        filterExpressionProcessor.getFilterResolver(filterExpression, absoluteTableIdentifier);

    //get splits based on filter
    return getSplits(job, filterResolver);
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException, IndexBuilderException {

    List<InputSplit> result = new LinkedList<InputSplit>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(getStorePathString(job), getTableToAccess(job));

    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getValidSegments(job)) {
      List<DataRefNode> dataRefNodes =
          getDataBlocksOfSegment(job, filterExpressionProcessor, absoluteTableIdentifier,
              filterResolver, segmentNo);
      for (DataRefNode dataRefNode : dataRefNodes) {
        BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
        TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
        result.add(new CarbonInputSplit(segmentNo, new Path(tableBlockInfo.getFilePath()),
            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
            tableBlockInfo.getLocations()));
      }
    }
    return result;
  }

  /**
   * get total number of rows. Same as count(*)
   * @throws IOException
   * @throws IndexBuilderException
   */
  public long getRowCount(JobContext job) throws IOException, IndexBuilderException {

    long rowCount = 0;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(getStorePathString(job), getTableToAccess(job));

    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getValidSegments(job)) {
      Map<String, AbstractIndex> segmentIndexMap =
          getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentNo);

      // sum number of rows of each task
      for (AbstractIndex abstractIndex : segmentIndexMap.values()) {
        rowCount += abstractIndex.getTotalNumberOfRows();
      }
    }
    return rowCount;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public FilterResolverIntf getResolvedFilter(JobContext job, Expression filterExpression)
      throws IOException, IndexBuilderException, QueryExecutionException {

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(getStorePathString(job), getTableToAccess(job));
    //get resolved filter
    return filterExpressionProcessor.getFilterResolver(filterExpression, absoluteTableIdentifier);
  }

  /**
   * get data blocks of given segment
   */
  private List<DataRefNode> getDataBlocksOfSegment(JobContext job,
      FilterExpressionProcessor filterExpressionProcessor,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver,
      String segmentId) throws IndexBuilderException, IOException {

    Map<String, AbstractIndex> segmentIndexMap =
        getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentId);

    List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

    // build result
    for (AbstractIndex abstractIndex : segmentIndexMap.values()) {

      List<DataRefNode> filterredBlocks = null;
      // if no filter is given get all blocks from Btree Index
      if(null == resolver){
        filterredBlocks = getDataBlocksOfIndex(abstractIndex);
      } else {
        // apply filter and get matching blocks
        filterredBlocks = filterExpressionProcessor
            .getFilterredBlocks(abstractIndex.getDataRefNode(), resolver, abstractIndex,
                absoluteTableIdentifier);
      }
      resultFilterredBlocks.addAll(filterredBlocks);
    }
    return resultFilterredBlocks;
  }

  private Map<String, AbstractIndex> getSegmentAbstractIndexs(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId)
      throws IOException, IndexBuilderException {
    Map<String, AbstractIndex> segmentIndexMap = SegmentTaskIndexStore.getInstance()
        .getSegmentBTreeIfExists(absoluteTableIdentifier, segmentId);

    // if segment tree is not loaded, load the segment tree
    if (segmentIndexMap == null) {
      // List<FileStatus> fileStatusList = new LinkedList<FileStatus>();
      List<TableBlockInfo> tableBlockInfoList = new LinkedList<TableBlockInfo>();
      // getFileStatusOfSegments(job, new int[]{ segmentId }, fileStatusList);

      // get file location of all files of given segment
      JobContext newJob =
          new JobContextImpl(new Configuration(job.getConfiguration()), job.getJobID());
      newJob.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, segmentId + "");

      // identify table blocks
      for (InputSplit inputSplit : getSplits(newJob)) {
        CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
        tableBlockInfoList.add(
            new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                segmentId, carbonInputSplit.getLocations(), carbonInputSplit.getLength()));
      }

      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
      segmentToTableBlocksInfos.put(segmentId, tableBlockInfoList);

      // get Btree blocks for given segment
      segmentIndexMap = SegmentTaskIndexStore.getInstance()
          .loadAndGetTaskIdToSegmentsMap(segmentToTableBlocksInfos, absoluteTableIdentifier);

    }
    return segmentIndexMap;
  }

  /**
   * get data blocks of given btree
   */
  private List<DataRefNode> getDataBlocksOfIndex(AbstractIndex abstractIndex) {
    List<DataRefNode> blocks = new LinkedList<DataRefNode>();
    SegmentProperties segmentProperties = abstractIndex.getSegmentProperties();

    try {
      IndexKey startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
      IndexKey endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);

      // Add all blocks of btree into result
      DataRefNodeFinder blockFinder = new BTreeDataRefNodeFinder(
          segmentProperties.getDimensionColumnsValueSize());
      DataRefNode startBlock =
          blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), startIndexKey);
      DataRefNode endBlock =
          blockFinder.findLastDataBlock(abstractIndex.getDataRefNode(), endIndexKey);
      while (startBlock != endBlock) {
        blocks.add(startBlock);
        startBlock = startBlock.getNextDataRefNode();
      }
      blocks.add(endBlock);

    } catch (KeyGenException e) {
      LOG.error("Could not generate start key", e);
    }
    return blocks;
  }

  @Override public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    //TODO: implement
    return null;
  }

  @Override protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
    return super.computeSplitSize(blockSize, minSize, maxSize);
  }

  @Override protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    return super.getBlockIndex(blkLocations, offset);
  }

  @Override protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    String[] segmentsToConsider = getValidSegments(job);
    if (segmentsToConsider.length == 0) {
      throw new IOException("No segments found");
    }

    getFileStatusOfSegments(job, segmentsToConsider, result);
    return result;
  }

  private void getFileStatusOfSegments(JobContext job, String[] segmentsToConsider,
      List<FileStatus> result) throws IOException {
    String[] partitionsToConsider = getValidPartitions(job);
    if (partitionsToConsider.length == 0) {
      throw new IOException("No partitions/data found");
    }

    PathFilter inputFilter = getDataFileFilter(job);
    CarbonTablePath tablePath = getTablePath(job);

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { tablePath },
        job.getConfiguration());

    //get all data files of valid partitions and segments
    for (int i = 0; i < partitionsToConsider.length; ++i) {
      String partition = partitionsToConsider[i];

      for (int j = 0; j < segmentsToConsider.length; ++j) {
        String segmentId = segmentsToConsider[j];
        Path segmentPath = new Path(tablePath.getCarbonDataDirectoryPath(partition, segmentId));
        FileSystem fs = segmentPath.getFileSystem(job.getConfiguration());

        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(segmentPath);
        while (iter.hasNext()) {
          LocatedFileStatus stat = iter.next();
          if (inputFilter.accept(stat.getPath())) {
            if (stat.isDirectory()) {
              addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
              result.add(stat);
            }
          }
        }
      }
    }
  }

  public Path getStorePath(JobContext job) throws IOException {

    String storePathString = getStorePathString(job);
    return new CarbonStorePath(storePathString);
  }

  public CarbonTablePath getTablePath(JobContext job) throws IOException {

    String storePathString = getStorePathString(job);
    CarbonTableIdentifier tableIdentifier = CarbonInputFormat.getTableToAccess(job);
    if (tableIdentifier == null) {
      throw new IOException("Could not find " + DATABASE_NAME + "," + TABLE_NAME);
    }
    return CarbonStorePath.getCarbonTablePath(storePathString, tableIdentifier);
  }

  /**
   * @param job
   * @return the PathFilter for Fact Files.
   */
  public PathFilter getDataFileFilter(JobContext job) {
    return new CarbonPathFilter(getUpdateExtension());
  }

  private String getStorePathString(JobContext job) throws IOException {

    String dirs = job.getConfiguration().get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    if (inputPaths.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    return inputPaths[0];
  }

  /**
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String getUpdateExtension() {
    // TODO: required to modify when supporting update, mostly will be update timestamp
    return "update";
  }

  /**
   * @return updateExtension
   */
  private String[] getValidSegments(JobContext job) throws IOException {
    String segmentString = job.getConfiguration().get(INPUT_SEGMENT_NUMBERS, "");
    // if no segments
    if(segmentString.trim().isEmpty()){
      return new int[0];
    }

    String[] segments = segmentString.split(",");

    return segments;
  }

  /**
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String[] getValidPartitions(JobContext job) {
    //TODO: has to Identify partitions by partition pruning
    return new String[] { "0" };
  }
}
