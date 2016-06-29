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
package org.carbondata.core.carbon.datastore.block;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class is responsible for maintaining the mapping of tasks of a node.
 */
public class TableTaskInfo extends Distributable {

  private final List<TableBlockInfo> tableBlockInfoList;
  private final String taskId;
  public String getTaskId() {
    return taskId;
  }

  public List<TableBlockInfo> getTableBlockInfoList() {
    return tableBlockInfoList;
  }

  public TableTaskInfo(String taskId, List<TableBlockInfo> tableBlockInfoList){
    this.taskId = taskId;
    this.tableBlockInfoList = tableBlockInfoList;
  }

  @Override public String[] getLocations() {
    Set<String> locations = new HashSet<String>();
    for(TableBlockInfo tableBlockInfo: tableBlockInfoList){
      locations.addAll(Arrays.asList(tableBlockInfo.getLocations()));
    }
    locations.toArray(new String[locations.size()]);
    List<String> nodes =  TableTaskInfo.maxNoNodes(tableBlockInfoList);
    return nodes.toArray(new String[nodes.size()]);
  }

  @Override public int compareTo(Distributable o) {
    return taskId.compareTo(((TableTaskInfo)o).getTaskId());
  }

  /**
   * Finding the maximum node which has the blocks.
   * @param blockList
   * @return
   */
  public static List<String> maxNoNodes(List<TableBlockInfo> blockList) {
    boolean useIndex = true;
    Integer maxOccurence = 0;
    String maxNode = null;
    Map<String, Integer> nodeAndOccurenceMapping = new TreeMap<>();
    for (TableBlockInfo block : blockList) {
      for (String node : block.getLocations()) {
        Integer nodeOccurence = nodeAndOccurenceMapping.get(node);
        if (null == nodeOccurence) {
          nodeAndOccurenceMapping.put(node, 1);
        } else {
          nodeOccurence++;
        }
      }
    }
    Integer previousValueOccurence = null;

    for (Map.Entry<String, Integer> entry : nodeAndOccurenceMapping.entrySet()) {
      if (entry.getValue() > maxOccurence) {
        maxOccurence = entry.getValue();
        maxNode = entry.getKey();
      }
      if (null == previousValueOccurence) {
        previousValueOccurence = entry.getValue();
      } else {
        if (previousValueOccurence != entry.getValue()) {
          useIndex = false;
        }
      }
    }

    if (useIndex) {
      return new ArrayList<>(nodeAndOccurenceMapping.keySet());
    }

    List<String> node =  new ArrayList<>(1);
    node.add(maxNode);
    return node;
  }
}
