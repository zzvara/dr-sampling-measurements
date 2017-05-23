package hu.sztaki.sm

import org.apache.commons.math3.distribution.ZipfDistribution

import scala.collection.mutable.ArrayBuffer

object ZipfTest {
  def main(args: Array[String]): Unit = {
    val exponent = 2.0
    var numberOfEvents = 0
    val zipf = new ZipfDistribution(1000 * 100, exponent)
    var key = 0
    println(s"Loading ZIPF-$exponent.")
    val myList = ArrayBuffer.empty[String]
    val data = (s"ZIPF-$exponent", {
      while (numberOfEvents < 4000000) {
        val zipfian = zipf.sample()
        numberOfEvents += zipfian
        val randomString = key.toString
        key += 1
        myList ++= List.fill[String](zipfian)(randomString)
      }
      println("Created.")
      scala.util.Random.shuffle(myList)
    })
    println(s"Size of data is [${data._2.size}].")
    data
  }
}
