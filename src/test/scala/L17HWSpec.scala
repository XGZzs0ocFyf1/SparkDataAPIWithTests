import L17HW.{mostPopularBoroughs, mostPopularDistance}
import model.{TaxiRide, TaxiZone}
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll

class L17HWSpec extends SharedSparkSession {
  import testImplicits._

  //tests with generated data
  test("most popular distance test") {
    import DataGenerator._
    implicit val taxiFacts: Arbitrary[TaxiRide] = Arbitrary(genTaxiFacts)

    forAll { (facts: Seq[TaxiRide]) =>
      val taxiFacts = facts.toDF()
      val result = mostPopularDistance(taxiFacts)

      val resDF = result.toDF("distance", " count")

      if (resDF.take(2).length == 2){ //пустых строк нет смысла сравнивать их, поэтому пропускаем этот случай
        val firstLine: Row = resDF.take(2).head
        val secondLine: Row = resDF.take(2).tail.head
        (firstLine.get(1).asInstanceOf[Int] <= secondLine.get(1).asInstanceOf[Int]) shouldBe true
      }
    }
  }


  test("popular boroughs test") {
    import DataGenerator._
    implicit val taxiZones: Arbitrary[TaxiZone] = Arbitrary(genTaxiZone)
    implicit val taxiFacts: Arbitrary[TaxiRide] = Arbitrary(genTaxiFacts)

    forAll { (zones: Seq[TaxiZone], facts: Seq[TaxiRide]) =>
      val zonesDF = zones.toDF()
      val factsDF = facts.toDF()

      val actualResult = mostPopularBoroughs(factsDF, zonesDF)

      //проверяем что выдается упорядоченный по количеству поездок список
      if(actualResult.count > 1) {
        val l1 = actualResult.take(2)(0)._2
        val l2 = actualResult.take(2)(1)._2
        (l1 >= l2) shouldBe true
      }
      val res = actualResult.collect()

      //все районы в списке разные
      (res.map(_._1).distinct.length == res.length ) shouldBe true
    }
  }

}
