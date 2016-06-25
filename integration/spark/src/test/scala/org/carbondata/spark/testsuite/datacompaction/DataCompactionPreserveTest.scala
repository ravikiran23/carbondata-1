package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

/**
  * FT for data compaction preserve scenario.
  */
class DataCompactionPreserveTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.numberof.preserve.segments", "2")

    sql("drop table if exists  preserveSegments")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")

    sql(
      "CREATE TABLE IF NOT EXISTS preserveSegments (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE preserveSegments PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE preserveSegments  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE preserveSegments  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE preserveSegments  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE preserveSegments  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    )
    // compaction will happen here.
    sql("alter table preserveSegments compact 'major'"
    )

  }

  test("check if compaction has preserved segments or not.") {
    var status = true
    var noOfRetries = 0
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "preserveSegments", "1")
        )
    )
    var segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList
    while (status && noOfRetries < 10) {


      if (!segments.contains("0.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(2000)
        noOfRetries += 1
        segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList
      }
      else {
        status = false
      }
    }
    segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList
    if (!status) {
      if (segments.contains("3") && segments.contains("4")) {
        assert(true);
      }
      else {
        assert(false);
      }
    }
    else {
      assert(false);
    }

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty("carbon.numberof.preserve.segments", "0")
  }

}
