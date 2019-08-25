package ingestion

import cats.implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transform._
import transformation.Transformations._
import transformation.transformations.{Base64, Select, Transformation}
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaGenerator._
import utils.test.schema.SchemaModels._
import transformation.transformations.TransformationUtils._
import Ingestion.Transformations._
import pureconfig.ConfigReader
import pureconfig.error.ConfigReaderFailures
import shapeless.{Poly1, _}
import transformation.transformations.TestingShapeless._


@RunWith(classOf[JUnitRunner])
class transformationsTest
    extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {

  import spark.implicits._
  import scala.collection.JavaConverters._
  test("write in base64") {



    val u = ozuna
    val opps=genu.from(ozuna)

    val config = ConfigFactory.parseString(s"""
       |transformations =
       |[
       |  {
       |    field = "A"
       |    type = "base64"
       |  },
       |  {
       |    columnsToSelect = ["A"]
       |    type = "select"
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

    class TransformationList[D <: Transformation](donuts: Seq[D])
    /** This function enables to convert a sequence of eithers into an either of a sequence. */
    def sequence[A, B](s: Seq[Either[A, B]]): Either[A, Seq[B]] =
      s.foldRight(Right(Nil): Either[A, List[B]]) { (e, acc) =>
        for (xs <- acc.right; x <- e.right) yield x :: xs
      }

    val transformationsu =
      sequence(config
        .getConfigList("transformations")
        .asScala
        .map(conf => getTransformation(conf).map(trans=> generic.from(generic.to(trans)))))

//    applycation(transformations.asRight: Either[ConfigReaderFailures, Seq[+Transformation]])

    import shapeless.{Coproduct, :+:, CNil, Inl, Inr}

    type CoProductoSolution = Base64 :+: Select :+: CNil

    object result extends Poly1 {
      implicit val base64: this.Case[Base64]{type Result=Base64}= at[Base64](t => t)
      implicit val select: this.Case[Select]{type Result=Select} = at[Select](t => t)
    }

    def getClass(name: String)= Class.forName(s"transformation.transformations.$name")



    import syntax.std.traversable._
    val act2 =getClass("Select")
    val o=transformationsu.map(sequence => sequence.toHList[Base64:: Select :: HNil])

//    val uhuhu=Coproduct[Producto](Base64(field="A"))
//
//    val x=transformations.map(result)



    print(transformationsu)

//    val dfCleaned = applyTransformations(df, sequence(transformations))(spark)

  }
}
