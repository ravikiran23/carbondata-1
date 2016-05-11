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

package org.carbondata.spark.merger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.lcm.status.SegmentStatusManager;
import org.carbondata.spark.load.CarbonLoadModel;
import org.carbondata.spark.load.CarbonLoaderUtil;
import org.carbondata.spark.load.DeleteLoadFolders;
import org.carbondata.spark.util.LoadMetadataUtil;

/**
 * utility class for load merging.
 */
public final class CarbonDataMergerUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataMergerUtil.class.getName());

  private CarbonDataMergerUtil() {

  }

  public static boolean executeMerging(CarbonLoadModel carbonLoadModel, String storeLocation,
      String hdfsStoreLocation, int currentRestructNumber, String metadataFilePath,
      List<String> loadsToMerge, String mergedLoadName) throws Exception {
    // TODO: Implement it
    return false;

  }

  public static List<String> getLoadsToMergeFromHDFS(String storeLocation, FileType fileType,
      String metadataPath, CarbonLoadModel carbonLoadModel, int currentRestructNumber,
      int partitionCount) {
    List<String> loadNames = new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    try {
      if (!FileFactory.isFileExist(storeLocation, fileType)) {
        return null;
      }
    } catch (IOException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }

    int toLoadMergeMaxSize;
    try {
      toLoadMergeMaxSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE,
              CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT));
    } catch (NumberFormatException e) {
      toLoadMergeMaxSize = Integer.parseInt(CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT);
    }

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(
        new AbsoluteTableIdentifier(
            storeLocation,
            new CarbonTableIdentifier(carbonLoadModel.getDatabaseName(),
                carbonLoadModel.getTableName())));

    LoadMetadataDetails[] loadDetails = segmentStatusManager.readLoadMetadata(metadataPath);

    for (LoadMetadataDetails loadDetail : loadDetails) {
      if (loadNames.size() < 2) {
        // check if load is not deleted.
        if (checkIfLoadIsNotDeleted(loadDetail)) {
          // check if load is merged
          if (checkIfLoadIsMergedAlready(loadDetail)) {
            if (checkSizeOfloadToMerge(loadDetail, toLoadMergeMaxSize, carbonLoadModel,
                partitionCount, storeLocation, currentRestructNumber,
                loadDetail.getMergedLoadName())) {
              if (!loadNames.contains(loadDetail.getMergedLoadName())) {
                loadNames.add(loadDetail.getMergedLoadName());
              }
            }
          } else
          // take this load as To Load.
          {
            if (checkSizeOfloadToMerge(loadDetail, toLoadMergeMaxSize, carbonLoadModel,
                partitionCount, storeLocation, currentRestructNumber, loadDetail.getLoadName())) {
              loadNames.add(loadDetail.getLoadName());
            }
          }
        }
      } else {
        break;
      }
    }

    return loadNames;

  }

  private static boolean checkSizeOfloadToMerge(final LoadMetadataDetails loadDetail,
      int toLoadMergeMaxSize, CarbonLoadModel carbonLoadModel, int partitionCount,
      String storeLocation, int currentRestructNumber, final String loadNameToMatch) {

    long factSizeAcrossPartition = 0;

    for (int partition = 0; partition < partitionCount; partition++) {
      String loadPath = LoadMetadataUtil
          .createLoadFolderPath(carbonLoadModel, storeLocation, partition, currentRestructNumber);

      CarbonFile parentLoadFolder =
          FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));
      CarbonFile[] loadFiles = parentLoadFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          if (file.getName().substring(file.getName().indexOf('_') + 1, file.getName().length())
              .equalsIgnoreCase(loadNameToMatch)) {
            return true;
          }
          return false;
        }
      });

      // no found load folder in current RS
      if (loadFiles.length == 0) {
        return false;
      }

      // check if fact file is present or not. this is in case of Restructure folder.
      if (!isFactFilePresent(loadFiles[0])) {
        return false;
      }
      factSizeAcrossPartition += getSizeOfFactFileInLoad(loadFiles[0]);
    }
    // check avg fact size if less than configured max size of to load.
    if (factSizeAcrossPartition < toLoadMergeMaxSize * 1024 * 1024 * 1024) {
      return true;
    }
    return false;
  }

  private static long getSizeOfFactFileInLoad(CarbonFile carbonFile) {
    long factSize = 0;

    // check if update fact is present.

    CarbonFile[] factFileUpdated = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
          return true;
        }
        return false;
      }
    });

    if (factFileUpdated.length != 0) {
      for (CarbonFile fact : factFileUpdated) {
        factSize += fact.getSize();
      }
      return factSize;
    }

    // normal fact case.
    CarbonFile[] factFile = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });

    for (CarbonFile fact : factFile) {
      factSize += fact.getSize();
    }

    return factSize;
  }

  private static boolean checkIfLoadIsMergedAlready(LoadMetadataDetails loadDetail) {
    if (null != loadDetail.getMergedLoadName()) {
      return true;
    }
    return false;
  }

  private static boolean checkIfLoadIsNotDeleted(LoadMetadataDetails loadDetail) {
    if (!loadDetail.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean checkIfLoadMergingRequired() {

    // check whether carbons segment merging operation is enabled or not.
    // default will be false.
    String isLoadMergeEnabled = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_LOAD_MERGE,
            CarbonCommonConstants.DEFAULT_ENABLE_LOAD_MERGE);

    if (isLoadMergeEnabled.equalsIgnoreCase("false")) {
      return false;
    }
    return true;

       /* // check whether the preserving of the segments from merging is enabled or not.
        String isPreserveSegmentEnabled = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS,
                        CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS);

        if (isPreserveSegmentEnabled.equalsIgnoreCase("true")) {

            // check whether the valid segments is more than that of the preserved segments.

            String loadPath = LoadMetadataUtil
                    .createLoadFolderPath(carbonLoadModel, storeLocation, 0, currentRestructNumber);

            CarbonFile parentLoadFolder =
                    FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));

            //get all the load files in the current RS

            CarbonFile[] loadFiles = parentLoadFolder.listFiles(new CarbonFileFilter() {
                @Override
                public boolean accept(CarbonFile file) {
                    if (file.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER)) {
                        return true;
                    }
                    return false;
                }
            });


            int mergeThreshold;
            try {
                mergeThreshold = Integer.parseInt(CarbonProperties.getInstance()
                        .getProperty(CarbonCommonConstants.MERGE_THRESHOLD_VALUE,
                                CarbonCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL));
            } catch (NumberFormatException e) {
                mergeThreshold = Integer.parseInt
                (CarbonCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL);
            }

    LoadMetadataDetails[] details = CarbonUtil.readLoadMetadata(metadataFilePath);

    int validLoadsNumber = getNumberOfValidLoads(details, loadFiles);

            if (validLoadsNumber > mergeThreshold + 1) {
                return true;
            } else {
                return false;
            }
        }*/
  }

  private static int getNumberOfValidLoads(LoadMetadataDetails[] details, CarbonFile[] loadFiles) {
    int validLoads = 0;

    for (LoadMetadataDetails load : details) {
      if (load.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
          || load.getLoadStatus()
          .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) || load
          .getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_UPDATE)) {

        if (isLoadMetadataPresentInRsFolders(loadFiles, load.getLoadName())) {
          validLoads++;
        }
      }
    }

    return validLoads;

  }

  private static boolean isLoadMetadataPresentInRsFolders(CarbonFile[] loadFiles, String loadName) {
    for (CarbonFile load : loadFiles) {
      String nameOfLoad = load.getName()
          .substring(load.getName().indexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              load.getName().length());
      if (nameOfLoad.equalsIgnoreCase(loadName)) {
        //check if it is a RS load or not.
        CarbonFile[] factFiles = load.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile file) {
            if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT) || file.getName()
                .endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
              return true;
            }
            return false;
          }
        });

        if (factFiles.length > 0) {
          return true;
        } else {
          return false;
        }
      }

    }
    return false;
  }

  public static String getMergedLoadName(List<LoadMetadataDetails> segmentsToBeMergedList) {
    String firstSegmentName = segmentsToBeMergedList.get(0).getLoadName();

    double segmentNumber = Double.parseDouble(
        firstSegmentName.substring(firstSegmentName.lastIndexOf('_'), firstSegmentName.length()));

    // increment segment number by 0.1

    segmentNumber += 0.1;

    return CarbonCommonConstants.LOAD_FOLDER + segmentNumber;

  }

  public static void updateLoadMetadataWithMergeStatus(List<String> loadsToMerge,
      String metaDataFilepath, String MergedLoadName, CarbonLoadModel carbonLoadModel) {

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(
        new AbsoluteTableIdentifier(
            carbonLoadModel.getFactStoreLocation(),
            new CarbonTableIdentifier(carbonLoadModel.getDatabaseName(),
                carbonLoadModel.getTableName())));
    LoadMetadataDetails[] loadDetails = segmentStatusManager.readLoadMetadata(metaDataFilepath);

    boolean first = true;

    for (LoadMetadataDetails loadDetail : loadDetails) {

      if (null != loadDetail.getMergedLoadName()) {
        if (loadsToMerge.contains(loadDetail.getMergedLoadName()) && first) {
          loadDetail.setMergedLoadName(MergedLoadName);
          first = false;
        } else {
          continue;
        }
      } else if (loadsToMerge.contains(loadDetail.getLoadName())) {
        if (first) {
          loadDetail.setMergedLoadName(MergedLoadName);
          first = false;
        } else {
          loadDetail.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
          loadDetail.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime());
        }

      }

    }

    try {
      CarbonLoaderUtil.writeLoadMetadata(carbonLoadModel.getCarbonDataLoadSchema(),
          carbonLoadModel.getDatabaseName(), carbonLoadModel.getTableName(),
          Arrays.asList(loadDetails));
    } catch (IOException e) {
      LOGGER.error("Error while writing metadata");
    }

  }

  public static void cleanUnwantedMergeLoadFolder(CarbonLoadModel loadModel, int partitionCount,
      String storeLocation, boolean isForceDelete, int currentRestructNumber) {

    CarbonTable cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable(loadModel.getDatabaseName() + '_' + loadModel.getTableName());

    String loadMetadataFilePath = cube.getMetaDataFilepath();
    //String loadMetadataFilePath = CarbonLoaderUtil.extractLoadMetadataFileLocation(loadModel);

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(
        new AbsoluteTableIdentifier(
            storeLocation,
            new CarbonTableIdentifier(loadModel.getDatabaseName(),
                loadModel.getTableName())));
    LoadMetadataDetails[] details = segmentStatusManager.readLoadMetadata(loadMetadataFilePath);

    // for first time before any load , this will be null
    if (null == details || details.length == 0) {
      return;
    }

    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {

      String path = LoadMetadataUtil
          .createLoadFolderPath(loadModel, storeLocation, partitionId, currentRestructNumber);

      CarbonFile loadFolder = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));

      CarbonFile[] loads = loadFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          if (file.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER) && file.getName()
              .contains(CarbonCommonConstants.MERGER_FOLDER_EXT)) {
            return true;
          } else {
            return false;
          }
        }
      });

      for (int i = 0; i < loads.length; i++) {
        if (checkIfOldMergeLoadCanBeDeleted(loads[i], details)) {
          // delete merged load folder
          CarbonFile[] files = loads[i].listFiles();
          // deleting individual files
          if (files != null) {
            for (CarbonFile eachFile : files) {
              if (!eachFile.delete()) {
                LOGGER.warn("Unable to delete the file." + loadFolder.getAbsolutePath());
              }
            }

            loads[i].delete();

          }

          // delete corresponding aggregate table.

          CarbonFile[] aggFiles = LoadMetadataUtil
              .getAggregateTableList(loadModel, storeLocation, partitionId, currentRestructNumber);
          DeleteLoadFolders.deleteAggLoadFolders(aggFiles, loads[i].getName());

        }
      }
    }
  }

  private static boolean checkIfOldMergeLoadCanBeDeleted(CarbonFile eachMergeLoadFolder,
      LoadMetadataDetails[] details) {
    boolean found = false;
    for (LoadMetadataDetails loadDetail : details) {
      if (null != loadDetail.getMergedLoadName() && (CarbonCommonConstants.LOAD_FOLDER + loadDetail
          .getMergedLoadName()).equalsIgnoreCase(eachMergeLoadFolder.getName())) {
        found = true;
        break;
      }
    }

    if (!found) {
      // check the query execution time out and check the time stamp on load and delete.

      String loadName = eachMergeLoadFolder.getName();
      long loadTime = Long.parseLong(loadName
          .substring(loadName.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              loadName.length()));
      long currentTime = new Date().getTime();

      long millis = getMaxQueryTimeOut();

      if ((currentTime - loadTime) > millis) {
        // delete that merge load folder
        return true;
      }
    }

    return false;
  }

  private static long getMaxQueryTimeOut() {
    int maxTime;
    try {
      maxTime = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME));
    } catch (NumberFormatException e) {
      maxTime = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
    }

    return maxTime * 60000;

  }

  private static boolean isFactFilePresent(CarbonFile carbonFile) {

    CarbonFile[] factFileUpdated = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
          return true;
        }
        return false;
      }
    });

    if (factFileUpdated.length != 0) {
      return true;
    }

    CarbonFile[] factFile = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });

    if (factFile.length != 0) {
      return true;
    }

    return false;
  }

  public static List<LoadMetadataDetails> identifySegmentsToBeMerged(String storeLocation,
      FileType fileType, String metadataPath, CarbonLoadModel carbonLoadModel,
      int currentRestructNumber, int partitionCount, long compactionSize) {

    CarbonTableIdentifier tableIdentifier =
        new CarbonTableIdentifier(carbonLoadModel.getDatabaseName(),
            carbonLoadModel.getTableName());

    // check preserve property and preserve the configured number of latest loads.

    List<LoadMetadataDetails> listOfSegmentsAfterPreserve =
        checkPreserveSegmentsPropertyReturnRemaining(carbonLoadModel);

    // identify the segments to merge based on the Size of the segments across partition.

    List<LoadMetadataDetails> listOfSegmentsBelowThresholdSize =
        identifySegmentsToBeMergedBasedOnSize(compactionSize, listOfSegmentsAfterPreserve,
            carbonLoadModel, partitionCount, storeLocation);

    // filter the segments if the compaction based on days is configured.

    List<LoadMetadataDetails> listOfSegmentsLoadedInSameDateInterval =
        identifySegmentsToBeMergedBasedOnLoadedDate(listOfSegmentsBelowThresholdSize);

    return listOfSegmentsLoadedInSameDateInterval;
  }

  /**
   * This method will return the list of loads which are loaded at the same interval.
   * This property is configurable.
   *
   * @param listOfSegmentsBelowThresholdSize
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnLoadedDate(
      List<LoadMetadataDetails> listOfSegmentsBelowThresholdSize) {

    List<LoadMetadataDetails> loadsOfSameDate =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // check whether the property is enabled or not
    String isDateConsideredForMerging = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_COMPACTION_BASED_ON_DATE,
            CarbonCommonConstants.DEFAULT_ENABLE_COMPACTION_BASED_ON_DATE);

    // if true then process loads according to the load date.
    if (isDateConsideredForMerging.equalsIgnoreCase(("true"))) {
      // filter loads based on the loaded date
      long numberOfDaysAllowedToMerge = 0;
      try {
        numberOfDaysAllowedToMerge = Long.parseLong(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.DAYS_ALLOWED_TO_COMPACT,
                CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT));
      } catch (NumberFormatException e) {
        numberOfDaysAllowedToMerge =
            Long.parseLong(CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT);
      }

      boolean first = true;
      Date segDate1 = null;
      for (LoadMetadataDetails segment : listOfSegmentsBelowThresholdSize) {

        if (first) {
          String baselineLoadStartTime = segment.getLoadStartTime();
          SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
          try {
            segDate1 = sdf.parse(baselineLoadStartTime);
          } catch (ParseException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Error while parsing segment start time", e);
          }
          first = false;
          loadsOfSameDate.add(segment);
          continue;
        }
        String segmentDate = segment.getLoadStartTime();
        SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
        Date segDate2 = null;
        try {
          segDate2 = sdf.parse(segmentDate);
        } catch (ParseException e) {
          LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
              "Error while parsing segment start time", e);
        }

        if (isTwoDatesPresentInRequiredRange(segDate1, segDate2, numberOfDaysAllowedToMerge)) {
          loadsOfSameDate.add(segment);
        }
        // if the load is beyond merged date.
        // then reset everything and continue search for loads.
        else if (loadsOfSameDate.size() < 2) {
          first = true;
          loadsOfSameDate.removeAll(loadsOfSameDate);
        } else { // case where a load is beyond merge date and there is at least 2 loads to merge.
          break;
        }
      }
    } else {
      return listOfSegmentsBelowThresholdSize;
    }

    return loadsOfSameDate;
  }

  /**
   * Method to check if the load dates are complied to the configured dates.
   *
   * @param segDate1
   * @param segDate2
   * @return
   */
  private static boolean isTwoDatesPresentInRequiredRange(Date segDate1, Date segDate2,
      long numberOfDaysAllowedToMerge) {

    // take 1 st date add the configured days .
    Calendar cal1 = Calendar.getInstance();
    cal1.set(segDate1.getYear(), segDate1.getMonth(), segDate1.getDate());
    Calendar cal2 = Calendar.getInstance();
    cal2.set(segDate2.getYear(), segDate2.getMonth(), segDate2.getDate());

    long diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();

    if ((diff / (24 * 60 * 60 * 1000)) <= numberOfDaysAllowedToMerge) {
      return true;

    }
    return false;
  }

  /**
   * Identify the segments to be merged based on the Size.
   *
   * @param compactionSize
   * @param listOfSegmentsAfterPreserve
   * @param carbonLoadModel
   * @param partitionCount
   * @param storeLocation
   * @return
   */
  private static List<LoadMetadataDetails> identifySegmentsToBeMergedBasedOnSize(
      long compactionSize, List<LoadMetadataDetails> listOfSegmentsAfterPreserve,
      CarbonLoadModel carbonLoadModel, int partitionCount, String storeLocation) {

    List<LoadMetadataDetails> segmentsToBeMerged =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    CarbonTableIdentifier tableIdentifier =
        new CarbonTableIdentifier(carbonLoadModel.getDatabaseName(),
            carbonLoadModel.getTableName());

    // variable to store one  segment size across partition.
    long sizeOfOneSegmentAcrossPartition = 0;

    // total length
    long totalLength = 0;
    // check size of each segment , sum it up across partitions
    for (LoadMetadataDetails segment : listOfSegmentsAfterPreserve) {
      String segId = "";
      // segment is already merged. take the merged folder.
      if (null != segment.getMergedLoadName()) {

        segId = segment.getMergedLoadName();

      } else {
        segId = segment.getLoadName();
      }
      // calculate size across partitions
      for (int partition = 0; partition < partitionCount; partition++) {

        String loadPath = CarbonLoaderUtil
            .getStoreLocation(storeLocation, tableIdentifier, segId, partition + "");

        CarbonFile segmentFolder =
            FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));

        long sizeOfEachSegment = getSizeOfFactFileInLoad(segmentFolder);

        sizeOfOneSegmentAcrossPartition += sizeOfEachSegment;
      }
      totalLength += sizeOfOneSegmentAcrossPartition;
      // if the total length is less than compaction size then consider for compaction.
      if (totalLength < (compactionSize * 1024 * 1024)) {
        segmentsToBeMerged.add(segment);
      } else if (segmentsToBeMerged.size() < 2) {
        // reset everything as do only continuous merge.
        totalLength = 0;
        segmentsToBeMerged.removeAll(segmentsToBeMerged);
        continue;
      }
      // after all partitions
      sizeOfOneSegmentAcrossPartition = 0;
    }

    return segmentsToBeMerged;
  }

  /**
   * @param carbonLoadModel
   * @return
   */
  private static List<LoadMetadataDetails> checkPreserveSegmentsPropertyReturnRemaining(
      CarbonLoadModel carbonLoadModel) {

    // check whether the preserving of the segments from merging is enabled or not.
    String isPreserveSegmentEnabled = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS,
            CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS);

    if (isPreserveSegmentEnabled.equalsIgnoreCase("true")) {
      int numberOfSegmentsToBePreserved = 0;
      // get the number of loads to be preserved.
      try {
        numberOfSegmentsToBePreserved = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.PRESERVE_LATEST_SEGMENTS_NUMBER,
                CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER));
      } catch (NumberFormatException e) {
        numberOfSegmentsToBePreserved =
            Integer.parseInt(CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER);
      }

      // get the number of valid segments and retain the latest loads from merging.
      return CarbonDataMergerUtil
          .getValidLoadDetailsWithRetaining(carbonLoadModel.getLoadMetadataDetails(),
              numberOfSegmentsToBePreserved);

    } else {
      // if no need to preserve any segments then return the valid segments which can be merged.
      // return valid segment details .
      return CarbonDataMergerUtil
          .getValidLoadDetailsWithRetaining(carbonLoadModel.getLoadMetadataDetails(), 0);
    }
  }

  private static List<LoadMetadataDetails> getValidLoadDetailsWithRetaining(
      List<LoadMetadataDetails> loadMetadataDetails, int numberOfSegToBeRetained) {

    List<LoadMetadataDetails> validList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails segment : loadMetadataDetails) {
      if (segment.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
          || segment.getLoadStatus()
          .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) || segment
          .getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_UPDATE)) {
        validList.add(segment);
      }
    }

    // handle the retaining of valid loads,

    // check if valid list is big enough for removing the number of seg to be retained.
    if (validList.size() > numberOfSegToBeRetained) {

      // after the sort remove the loads from the last as per the retaining count.
      Collections.sort(validList, new Comparator<LoadMetadataDetails>() {

        @Override public int compare(LoadMetadataDetails seg1, LoadMetadataDetails seg2) {
          double segNumber1 = Double.parseDouble(seg1.getLoadName());
          double segNumber2 = Double.parseDouble(seg2.getLoadName());

          if ((segNumber1 - segNumber2) < 0) {
            return -1;
          } else if ((segNumber1 - segNumber2) > 0) {
            return 1;
          }
          return 0;

        }
      });

      for (int i = 0; i < numberOfSegToBeRetained; i++) {

        // remove last segment
        validList.remove(validList.size() - 1);

      }
    }

    return validList;
  }

  /**
   * This will give the compaction sizes configured based on compaction type.
   *
   * @param compactionType
   * @return
   */
  public static long getCompactionSize(CompactionType compactionType) {

    long compactionSize = 0;

    switch (compactionType) {
      case MINOR_COMPACTION:
        try {
          compactionSize = Long.parseLong(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.MINOR_COMPACTION_SIZE,
                  CarbonCommonConstants.DEFAULT_MINOR_COMPACTION_SIZE));
        } catch (NumberFormatException e) {
          compactionSize = Long.parseLong(CarbonCommonConstants.DEFAULT_MINOR_COMPACTION_SIZE);
        }
        break;

      case MAJOR_COMPACTION:
        try {
          compactionSize = Long.parseLong(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.MAJOR_COMPACTION_SIZE,
                  CarbonCommonConstants.DEFAULT_MAJOR_COMPACTION_SIZE));
        } catch (NumberFormatException e) {
          compactionSize = Long.parseLong(CarbonCommonConstants.DEFAULT_MAJOR_COMPACTION_SIZE);
        }
        break;
      default: // this case can not come.

    }
    return compactionSize;
  }
}
