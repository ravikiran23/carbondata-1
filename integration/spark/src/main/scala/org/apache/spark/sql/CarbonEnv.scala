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

package org.apache.spark.sql

import org.apache.spark.sql.hive.{CarbonMetastoreCatalog, HiveContext}

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.locks.ZookeeperInit
import org.carbondata.core.util.CarbonProperties

/**
 * Carbon Environment for unified context
 */
case class CarbonEnv(carbonContext: HiveContext, carbonCatalog: CarbonMetastoreCatalog)

object CarbonEnv {
  val className = classOf[CarbonEnv].getCanonicalName
  var carbonEnv: CarbonEnv = _

  def getInstance(sqlContext: SQLContext): CarbonEnv = {
    if (carbonEnv == null) {
      carbonEnv =
        CarbonEnv(sqlContext.asInstanceOf[CarbonContext],
          sqlContext.asInstanceOf[CarbonContext].catalog)

      // creating zookeeper instance once.
      // if zookeeper is configured as carbon lock type.
      if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT)
        .equalsIgnoreCase(CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER)) {
        val zookeeperUrl = sqlContext.getConf("spark.deploy.zookeeper.url", "")
        ZookeeperInit.getInstance(zookeeperUrl)

      }
    }
    carbonEnv
  }
}


