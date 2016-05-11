package org.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;

/**
 *
 */
public class CarbonCompactionUtil {

  /**
   * To create a mapping of Segment Id and TableBlockInfo.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, Map<String, List<TableBlockInfo>>> createMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList) {

    Map<String, Map<String, List<TableBlockInfo>>> segmentBlockInfoMapping =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (TableBlockInfo info : tableBlockInfoList) {
      List<TableBlockInfo> eachSegmentBlocks = new ArrayList<>();
      String segId = info.getSegmentId();
      // check if segId is already present in map
      Map<String, List<TableBlockInfo>> taskMapping = segmentBlockInfoMapping.get(segId);
      // extract task ID from file Path.
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(info.getFilePath());
      if (null == taskMapping) {
        taskMapping = new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        groupCorrespodingInfoBasedOnTask(info, taskMapping, taskNo);

        //put the inner Map to outer Map
        segmentBlockInfoMapping.put(segId, taskMapping);
      } else  // if task map it self is not there.
      {
        groupCorrespodingInfoBasedOnTask(info, taskMapping, taskNo);
      }
    }
    return segmentBlockInfoMapping;

  }

  private static void groupCorrespodingInfoBasedOnTask(TableBlockInfo info,
      Map<String, List<TableBlockInfo>> taskMapping, String taskNo) {
    // get the corresponding list from task mapping.
    List<TableBlockInfo> blockLists;
    if (null != taskMapping.get(taskNo)) {
      blockLists = taskMapping.get(taskNo);
      blockLists.add(info);
    } else {
      blockLists = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      blockLists.add(info);
      taskMapping.put(taskNo, blockLists);
    }
  }

  /**
   * To create a mapping of Segment Id and DataFileMetadata.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, List<DataFileFooter>> createDataFileMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList) throws IndexBuilderException {

    Map<String, List<DataFileFooter>> segmentBlockInfoMapping = new HashMap<>();
    for (TableBlockInfo blockInfo : tableBlockInfoList) {
      List<DataFileFooter> eachSegmentBlocks = new ArrayList<>();
      String segId = blockInfo.getSegmentId();

      DataFileFooter dataFileMatadata = null;
      // check if segId is already present in map
      List<DataFileFooter> metadataList = segmentBlockInfoMapping.get(segId);
      try {
        dataFileMatadata = CarbonUtil
            .readMetadatFile(blockInfo.getFilePath(), blockInfo.getBlockOffset(),
                blockInfo.getBlockLength());
        // dataFileMatadata.set(blockInfo.getFilePath());
      } catch (CarbonUtilException e) {
        throw new IndexBuilderException(e);
      }
      if (null == metadataList) {
        // if it is not present
        eachSegmentBlocks.add(dataFileMatadata);
        segmentBlockInfoMapping.put(segId, eachSegmentBlocks);
      } else {

        // if its already present then update the list.
        metadataList.add(dataFileMatadata);
      }
    }
    return segmentBlockInfoMapping;

  }

}
