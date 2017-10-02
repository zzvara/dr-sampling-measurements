package hu.sztaki.sm

import org.apache.commons.math3.distribution.ZipfDistribution

object ZipfTest {
  def main(args: Array[String]): Unit = {
    val exponent = 1.0
      println(s"Loading ZIPF-$exponent.")
    val zipf = new ZipfDistribution(100 * 1000, exponent)
    var key = 0
    val data = {
      (1 to 4).flatMap { _ =>
        scala.util.Random.shuffle({
          var numberOfEvents = 0
          (1 to (1000 * 1000)).flatMap { _ =>
            if (numberOfEvents < 1000 * 1000) {
              val zipfian = zipf.sample()
              numberOfEvents += zipfian
              val randomString = key.toString
              key += 1
              (1 to zipfian).map(_ => randomString).toIterator
            } else {
              Iterator.empty
            }
          }
        })
      }
    }
    data.foreach(println)
  }
}
