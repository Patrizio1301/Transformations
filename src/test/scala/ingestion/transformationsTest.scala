package ingestion

import cats.implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import utils.test.schema.SchemaModels._
import Ingestion.Transformations._
import utils.test.InitSparkSessionFunSuite

@RunWith(classOf[JUnitRunner])
class transformationsTest
    extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {
  test("write in base64") {

    val config = ConfigFactory.parseString(
      s"""
         |transformations =
         |[
         |  {
         |    field = "A"
         |    type = "base64"
         |  },
         |  {
         |    columnsToSelect = ["A"]
         |    type = "selectcolumns"
         |  }
         |]
      """.stripMargin)

    val columnToCheck = Seq(
      Row("aa", "bb"),
      Row(" aa  ", "ad"),
      Row(null, null),
      Row("  bb  ", "huhu"),
      Row(null, "jj"),
      Row("  cc  ", "jeje"),
      Row(null, null),
      Row(" dd", null)
    )

    lazy val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(2, StringType))

    readTransformations(config).map{
      transformations => applyTransformations(df, transformations)(spark).map(df => df.show())
    }
  }
}
