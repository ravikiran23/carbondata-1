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

package org.carbondata.integration.spark.rdd

import java.text.SimpleDateFormat
import java.util.{Date, List}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.carbon.datastore.BTreeBuilderInfo
import org.carbondata.core.carbon.datastore.block.{SegmentProperties, TableBlockInfo}
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.carbondata.integration.spark.MergeResult
import org.carbondata.integration.spark.load._
import org.carbondata.integration.spark.merger.{CarbonCompactionExecutor, CarbonCompactionUtil}
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.CarbonQueryUtil
import org.carbondata.query.carbon.result.RowResult

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
  factTableName: String,
  storePath: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      val split = theSplit.asInstanceOf[CarbonLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionId = split.serializableHadoopSplit.value.getPartition().getUniqueID()
      val model = carbonLoadModel
        .getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID())

      // sorting the table block info List.
      java.util.Collections.sort(tableBlockInfoList)

      val segmentMapping: java.util.Map[String, java.util.Map[String, List[TableBlockInfo]]] =
        CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)

      val dataFileMetadataSegMapping: java.util.Map[String, List[DataFileFooter]] =
        CarbonCompactionUtil.createDataFileMappingForSegments(tableBlockInfoList)


      // val cc:CarbonCompactor = new CarbonCompactor(dataFileMetadataSegMapping)

      // cc.process()

      val listMetadata = dataFileMetadataSegMapping.get(0)

      val segmentProperties = new SegmentProperties(listMetadata.get(0).getColumnInTable,
        listMetadata.get(0).getSegmentInfo.getColumnCardinality
      )


      val exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties, schemaName,
        factTableName, storePath
      )

      // fire a query and get the results.
      exec.processTableBlocks();

      // merge the result using a merger.

      // TODO remove comment below
      // val  resultList:List[CarbonIterator[Result]]  = exec.processTableBlocks()

      // = CarbonCompactionUtil.processTableBlocks(segmentMapping,queryExecutor,segmentProperties,
      // schemaName,factTableName)


      // create a segment builder info
      val indexBuilderInfo: BTreeBuilderInfo = new BTreeBuilderInfo(listMetadata,
        segmentProperties.getDimensionColumnsValueSize
      )


      //  old code
      val mergedLoadMetadataDetails = false
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
          finished = !false
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        result.getKey(0, mergedLoadMetadataDetails)
      }


      def checkAndLoadAggregationTable(): String = {
        var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
        val schema = model.getSchema
        val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toSeq.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + mergedLoadName
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil
            .addNewSliceNameToList(newSlice, listOfAllLoadFolders);
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          val copyListOfLoadFolders = listOfLoadFolders.asScala.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.asScala.toList
          loadCubeSlices(listOfAllLoadFolders, details)
          var loadFolders = Array[String]()
          val loadFolder = CarbonLoaderUtil
            .getAggLoadFolderLocation(newSlice, model.getDatabaseName, model.getTableName,
              model.getTableName, hdfsStoreLocation, currentRestructNumber
            )
          if (null != loadFolder) {
            loadFolders :+= loadFolder
          }
          dataloadStatus = iterateOverAggTables(aggTables, copyListOfLoadFolders.asJava,
            copyListOfUpdatedLoadFolders.asJava, loadFolders
          )
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            CarbonLoaderUtil
              .removeSliceFromMemory(model.getDatabaseName, model.getTableName, newSlice)
            logInfo(s"Aggregate table creation failed")
          } else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        dataloadStatus
      }


      def loadCubeSlices(listOfLoadFolders: java.util.List[String],
        deatails: Array[LoadMetadataDetails]) = {
        CarbonProperties.getInstance().addProperty("carbon.cache.used", "false");
        CarbonQueryUtil.createDataSource(currentRestructNumber, model.getSchema, null, partitionId,
          listOfLoadFolders, model.getTableName, hdfsStoreLocation, cubeCreationTime, deatails
        )
      }

      def iterateOverAggTables(aggTables: Array[CarbonDef.AggTable],
        listOfLoadFolders: java.util.List[String],
        listOfUpdatedLoadFolders: java.util.List[String],
        loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.foreach { aggTable =>
          val aggTableName = CarbonLoaderUtil.getAggregateTableName(aggTable)
          model.setAggTableName(aggTableName)
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders,
            loadFolders
          )
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: $aggTableName")
            return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          }
        }
        CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String],
        listOfUpdatedLoadFolders: java.util.List[String],
        loadFolders: Array[String]): String = {
        loadFolders.foreach { loadFolder =>
          val restructNumber = CarbonUtil.getRestructureNumber(loadFolder, model.getTableName)
          try {
            if (CarbonLoaderUtil
              .isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders,
                model.getTableName
              )) {
              model.setFactStoreLocation(loadFolder)
              CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath,
                restructNumber
              )
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
            } else {
              CarbonLoaderUtil
                .createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
            }
          } catch {
            case e: Exception => dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } finally {
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val loadName = loadFolder
                .substring(loadFolder.indexOf(CarbonCommonConstants.LOAD_FOLDER))
              CarbonLoaderUtil.copyCurrentLoadToHDFS(model, loadName, listOfUpdatedLoadFolders)
            } else {
              logInfo(s"Load creation failed :: $loadFolder")
              return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
            }
          }
        }
        return CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      }


    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations.asScala
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  override def getPartitions: Array[Partition] = {
    val carbonInputFormat = new CarbonInputFormat[RowResult]();
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)

    // FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getStorePath))
    // CarbonInputFormat.setTableToAccess(job, absoluteTableIdentifier.getCarbonTableIdentifier)
    // CarbonInputFormat.setSegmentsToAccess(job, validSegmentNos.asJava)

    val splits = carbonInputFormat.getSplits(job)
    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
    carbonInputSplits(0).getLocations;
    val result = new Array[Partition](splits.size)
    for (i <- 0 until result.length) {
      // result(i) = new CarbonLoadPartition(id, i, carbonInputSplits(i))
    }
    result
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
