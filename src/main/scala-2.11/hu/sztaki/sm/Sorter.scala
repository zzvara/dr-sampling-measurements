package hu.sztaki.sm

import org.apache.spark.{SparkConf, SparkContext}

object Sorter {
  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf()
      .setAppName("sorter")
      .setMaster("local[2]")
    val context = new SparkContext(configuration)
      .textFile("C:\\Users\\Ehnalis\\Downloads\\timeseries.csv", 10)
      .map { line =>
        line.split("""\|""")(6)
      }
      .flatMap {
        tags =>
          tags.split(",")
      }
      .map {
        t =>
          (t, 1)
      }
      .reduceByKey(_ + _)
      .flatMap(c => (1 to c._2).map(_ => (c._1, c._2)))
      .sortBy(_._2)
      .saveAsTextFile("C:\\Users\\Ehnalis\\Downloads\\timeseries-sorted-increasing")
  }
}
