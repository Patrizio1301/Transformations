package utils.test

import java.net.URL

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import utils.test.KirbySuiteContextProvider

trait InitSparkSessionFunSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  lazy val spark: SparkSession = KirbySuiteContextProvider.getOptSparkSession.get

  var result: Boolean  = _
  var TestPlanId: Int  = _
  var testCase: String = _

  val MOCK_REPORT = "mockReport"

  override def beforeEach(): Unit = {
    result = false
    testCase = ""
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (KirbySuiteContextProvider._spark != None.orNull &&
        KirbySuiteContextProvider._spark.sparkContext != None.orNull) {

      KirbySuiteContextProvider._spark.sparkContext.stop()
    }
    KirbySuiteContextProvider._spark = None.orNull
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    KirbySuiteContextProvider._spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read File")
      .getOrCreate()

    if (Option(System.getenv(MOCK_REPORT)).isDefined && !System.getenv(MOCK_REPORT).toBoolean) {
      // Get Testlink URL
      val url: URL = new URL(System.getenv("url"))
    }
  }

  def createDataFrame(content: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(content), schema)

  def getColumnAsSet[T](df: DataFrame, column: String): Set[T] =
    df.select(column).collect.map(r => r.getAs[T](column.split('.').last)).toSet
}
