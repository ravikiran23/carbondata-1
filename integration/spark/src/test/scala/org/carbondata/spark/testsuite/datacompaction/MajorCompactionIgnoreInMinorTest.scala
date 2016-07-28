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
  * FT for compaction scenario where major segment should not be included in minor.
  */
class MajorCompactionIgnoreInMinorTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.compaction.level.threshold", "2,2")
    sql("drop table if exists  ignoreMajor")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    sql(
      "CREATE TABLE IF NOT EXISTS ignoreMajor (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    val csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    val csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoreMajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoreMajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table ignoreMajor compact 'major'"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE ignoreMajor OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE ignoreMajor  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("alter table ignoreMajor compact 'minor'"
    )
    Thread.sleep(5000)
    sql("alter table ignoreMajor compact 'minor'"
    )
  }

 /* /**
    * Check if the compaction is completed or not.
    *
    * @param requiredSeg
    * @return
    */
  def checkCompactionCompletedOrNot(requiredSeg: String): Boolean = {
    var status = false
    var noOfRetries = 0
    while (!status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "ignoreMajor", noOfRetries + "")
          )
      )
      val segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList
      System.out.println( "Segments =" + segments)
      segments.foreach(seg =>
        System.out.println( "valid segment is =" + seg)
      )

      if (!segments.contains(requiredSeg)) {
        // wait for 2 seconds for compaction to complete.
        System.out.println("sleping for 2 seconds.")
        Thread.sleep(2000)
        noOfRetries += 1
      }
      else {
        status = true
      }
    }
    return status
  }*/

  test("check if compaction is completed or not.") {
    var status = true
    var noOfRetries = 0
    while (status && noOfRetries < 10) {

      val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
          AbsoluteTableIdentifier(
            CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
            new CarbonTableIdentifier("default", "ignoreMajor", "1")
          )
      )
      val segments = segmentStatusManager.getValidSegments().listOfValidSegments.asScala.toList

      if (!segments.contains("2.1")) {
        // wait for 2 seconds for compaction to complete.
        Thread.sleep(2000)
        noOfRetries += 1
      }
      else {
        status = false
      }
    }
  }

  /**
    * Test whether major compaction is not included in minor compaction.
    */
  test("delete merged folder and check segments") {
    // delete merged segments
    sql("clean files for table ignoreMajor")

    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(new
        AbsoluteTableIdentifier(
          CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier("default", "ignoreMajor", "rrr")
        )
    )
    // merged segment should not be there
    val segments = segmentStatusManager.getValidSegments.listOfValidSegments.asScala.toList
    assert(segments.contains("0.1"))
    assert(segments.contains("2.1"))
    assert(!segments.contains("2"))
    assert(!segments.contains("3"))

  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

}
