package org.carbondata.spark.testsuite.datacompaction

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * FT for data compaction scenario.
  */
class dataCompactionTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "true")
    sql("drop table if exists  normalcompaction")

    sql(
      "CREATE TABLE IF NOT EXISTS normalcompaction (country String, ID Int, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    var csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compaction2.csv"
    var csvFilePath3 = currentDirectory + "/src/test/resources/compaction/compaction3.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql("LOAD DATA fact from '" + csvFilePath1 + "' INTO CUBE normalcompaction PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    );
    sql("LOAD DATA fact from '" + csvFilePath2 + "' INTO CUBE normalcompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    );
    // compaction will happen here.
    sql("LOAD DATA fact from '" + csvFilePath3 + "' INTO CUBE normalcompaction  PARTITIONDATA" +
      "(DELIMITER ',', QUOTECHAR '\"')"
    );

  }


  test("select country from normalcompaction") {
    // check answers after compaction.
    checkAnswer(
      sql("select country from normalcompaction"),
      Seq(Row("america"),
        Row("canada"),
        Row("chile"),
        Row("china"),
        Row("england"),
        Row("burma"),
        Row("butan"),
        Row("mexico"),
        Row("newzealand"),
        Row("westindies"),
        Row("china"),
        Row("india"),
        Row("iran"),
        Row("iraq"),
        Row("ireland")
      )
    )
  }

  override def afterAll {
    sql("drop cube normalcompaction")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    CarbonProperties.getInstance().addProperty("carbon.enable.load.merge", "false")
  }

}
