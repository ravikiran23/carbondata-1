/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbondata.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, List}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.carbondata.core.carbon.datastore.BTreeBuilderInfo
import org.carbondata.core.carbon.datastore.block.{SegmentProperties, TableBlockInfo}
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonDef, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.iterator.CarbonIterator
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.carbondata.integration.spark.MergeResult
import org.carbondata.integration.spark.load._
import org.carbondata.integration.spark.merger.{CarbonCompactionExecutor, CarbonCompactionUtil, RowResultMerger}
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.CarbonQueryUtil
import org.carbondata.lcm.status.SegmentStatusManager
import org.carbondata.query.carbon.result.{BatchRawResult, RowResult}

import scala.collection.JavaConverters._

class CarbonMergerRDD[K, V](
  sc: SparkContext,
  result: MergeResult[K, V],
  carbonLoadModel: CarbonLoadModel,
  storeLocation: String,
  hdfsStoreLocation: String,
  partitioner: Partitioner,
  currentRestructNumber: Integer,
  metadataFilePath: String,
  loadsToMerge: List[LoadMetadataDetails],
  mergedLoadName: String,
  kettleHomePath: String,
  cubeCreationTime: Long,
  tableBlockInfoList: List[TableBlockInfo],
  schemaName: String,
  factTableName: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
     // val split = theSplit.asInstanceOf[CarbonSparkPartition]
     // logInfo("Input split: " + split.serializableHadoopSplit.value)
    //  val partitionId = split.serializableHadoopSplit.value.getPartition().getUniqueID()
    /*  val model = carbonLoadModel
        .getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID())*/
      carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
      val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]
      val carbonInputSplit = carbonSparkPartition.serializableHadoopSplit.value

      val tempLocationKey:String = carbonLoadModel.getDatabaseName + '_' + carbonLoadModel.getTableName;
      CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);

      val tableBlockInfoList1 = new util.ArrayList[TableBlockInfo]();
      tableBlockInfoList1.add(new TableBlockInfo("file:/D:\\carbondata\\examples\\target\\store3" +
        "\\default\\t6\\Fact\\Part0\\Segment_0/part-0-0-1463141747000.carbondata",
        carbonInputSplit.getStart,
        "0", carbonInputSplit.getLocations, carbonInputSplit.getLength
      ))
      tableBlockInfoList1.add(new TableBlockInfo(carbonInputSplit.getPath.toString,
          carbonInputSplit.getStart,
          carbonInputSplit.getSegmentId, carbonInputSplit.getLocations, carbonInputSplit.getLength
      ))

      // sorting the table block info List.
      //Collections.sort(tableBlockInfoList1)
      var segmentStatusManager = new SegmentStatusManager(new AbsoluteTableIdentifier
      (CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier(model.getDatabaseName, model.getTableName)))
      model.setLoadMetadataDetails(segmentStatusManager
        .readLoadMetadata(metadataFilePath).toList.asJava)

      val segmentMapping: java.util.Map[String, java.util.Map[String, List[TableBlockInfo]]] =
        CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList1)

      val dataFileMetadataSegMapping: java.util.Map[String, List[DataFileFooter]] =
      CarbonCompactionUtil.createDataFileMappingForSegments(tableBlockInfoList1)


      // val cc:CarbonCompactor = new CarbonCompactor(dataFileMetadataSegMapping)

      // cc.process()

      val listMetadata = dataFileMetadataSegMapping.get("0")

      val colCardinality : Array[Int] = listMetadata.get(listMetadata.size()-1).getSegmentInfo.getColumnCardinality

      val segmentProperties = new SegmentProperties(
        listMetadata.get(listMetadata.size()-1).getColumnInTable,
        colCardinality
      )


      val exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties, schemaName,
        factTableName, hdfsStoreLocation, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      )

      // fire a query and get the results.
      val result2: util.List[CarbonIterator[BatchRawResult]] = exec.processTableBlocks();

      val tempStoreLoc = CarbonCompactionUtil.getTempLocation(schemaName,factTableName,
        "0",mergedLoadName.substring
        (mergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER)+
          CarbonCommonConstants.LOAD_FOLDER.length(),mergedLoadName.length()),
        carbonLoadModel.getTaskNo)

     val merger = new  RowResultMerger(result2,
       factTableName,
        schemaName,
       hdfsStoreLocation,
       segmentProperties.getDimColumnsCardinality,
       segmentProperties.getDimColumnsCardinality.size,
       factTableName,
       0,
       segmentProperties,
       tempStoreLoc,
       carbonLoadModel,
       colCardinality
      )
      val mergeStatus = merger.mergerSlice()

    //  CarbonLoaderUtil.copyMergedLoadToHDFS(carbonLoadModel, currentRestructNumber, mergedLoadName)

      // merge the result using a merger.

      // TODO remove comment below
      // val  resultList:List[CarbonIterator[Result]]  = exec.processTableBlocks()

      // = CarbonCompactionUtil.processTableBlocks(segmentMapping,queryExecutor,segmentProperties,
      // schemaName,factTableName)


      // create a segment builder info
      /*val indexBuilderInfo: BTreeBuilderInfo = new BTreeBuilderInfo(listMetadata,
        segmentProperties.getDimensionColumnsValueSize
      )*/


      //  old code
      //val mergedLoadMetadataDetails = false
      /*  val mergedLoadMetadataDetails = CarbonDataMergerUtil
         .executeMerging(model, storeLocation, hdfsStoreLocation, currentRestructNumber,
           metadataFilePath, loadsToMerge, mergedLoadName)*/

      /*  model.setLoadMetadataDetails(CarbonUtil
         .readLoadMetadata(metadataFilePath).toList.asJava);

       if (mergedLoadMetadataDetails == true) {
         CarbonLoaderUtil.copyMergedLoadToHDFS(model, currentRestructNumber, mergedLoadName)
         dataloadStatus = checkAndLoadAggregationTable

       }*/

      var havePair = false
      var finished = false


      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        result.getKey(0, mergeStatus)
      }

    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")
  }

  override def getPartitions: Array[Partition] = {
    val carbonInputFormat = new CarbonInputFormat[RowResult]();
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)

    val carbonTableIdentifier:CarbonTableIdentifier = new CarbonTableIdentifier(carbonLoadModel
      .getDatabaseName, carbonLoadModel.getTableName
    )

     FileInputFormat.addInputPath(job, new Path(hdfsStoreLocation))
     CarbonInputFormat.setTableToAccess(job, carbonTableIdentifier)

    val absoluteTableIdentifier:AbsoluteTableIdentifier = new
        AbsoluteTableIdentifier(hdfsStoreLocation,carbonTableIdentifier);
    val validSegments = new SegmentStatusManager(absoluteTableIdentifier).getValidSegments;
    val validSegmentNos =
      validSegments.listOfValidSegments.asScala.map(x => new Integer(Integer.parseInt(x)))


     CarbonInputFormat.setSegmentsToAccess(job, validSegmentNos.asJava)

    val splits = carbonInputFormat.getSplits(job)
    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
    carbonInputSplits(0).getLocations;
    val result = new Array[Partition](splits.size)
    for (i <- 0 until result.length) {
       result(i) = new CarbonSparkPartition(id, i, carbonInputSplits(i))
    }
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }
}

class CarbonLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}
