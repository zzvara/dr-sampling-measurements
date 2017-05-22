package hu.sztaki.sm

import com.github.tototoshi.csv.{CSVFormat, CSVReader, QUOTE_MINIMAL, Quoting}
import frequencycount.lossycounting.LossyCountingModel
import hu.sztaki.drc
import hu.sztaki.sm.Baseline.Configuration.Adaptive
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.distribution.ZipfDistribution
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

object Baseline {
  object Configuration {
    abstract class Configuration

    case class Lossy(
      frequency: Double,
      error: Double,
      windows: Int = 1)
    extends Configuration {
      override def toString: String = {
        scala.runtime.ScalaRunTime._toString(this)
      }
    }

    case class Dynamic(
      histogramScaleBoundary: Int = 20,
      backoffFactor: Double = 2.0,
      histogramSizeBoundary: Int = 80,
      histogramCompation: Int = 40)
    extends Configuration

    case class Conceptier(
      take: Int = 20,
      histogramSoftBoundary: Int = 20,
      backoffFactor: Double = 2.0,
      histogramHardBoundary: Int = 80,
      histogramCompaction: Int = 40,
      driftBoundary: Double = 0.5,
      conceptSolidarity: Int = 5,
      driftHistoryWeight: Double = 0.8)
    extends Configuration {
      override def toString: String = {
        scala.runtime.ScalaRunTime._toString(this)
      }
    }
    case class Naive(
      take: Int = 20,
      histogramSoftBoundary: Int = 20,
      backoffFactor: Double = 2.0,
      histogramHardBoundary: Int = 80,
      histogramCompaction: Int = 40)
      extends Configuration {
      override def toString: String = {
        scala.runtime.ScalaRunTime._toString(this)
      }
    }
    case class Adaptive(
       take: Int = 20,
       backoffFactor: Double = 1.05)
      extends Configuration {
      override def toString: String = {
        scala.runtime.ScalaRunTime._toString(this)
      }
    }
  }

  def run[T : Manifest](
      data: List[T],
      configuration: Configuration.Configuration,
      rounds: Int = 10,
      runs: Int = 30): List[(Configuration.Configuration, Long, Map[Any, Double], Option[Any])] = {
    val methodFactory = configuration match {
      case c: Configuration.Lossy =>
        () => new LossyCountingModel[T](c.frequency, c.error)
      case c: Configuration.Conceptier =>
        () => new hu.sztaki.drc.Conceptier {
          override protected val TAKE: Int =
            c.take
          override protected val HISTOGRAM_SOFT_BOUNDARY: Int =
            c.histogramSoftBoundary
          override protected val BACKOFF_FACTOR: Double =
            c.backoffFactor
          protected var HISTOGRAM_HARD_BOUNDARY =
            c.histogramHardBoundary
          override protected val INITIAL_HARD_BOUNDARY =
            c.histogramHardBoundary
          override protected val HISTOGRAM_COMPACTION: Int =
            c.histogramCompaction
          override protected val DRIFT_BOUNDARY: Double =
            c.driftBoundary
          override protected val CONCEPT_SOLIDARITY: Int =
            c.conceptSolidarity
          override protected val DRIFT_HISTORY_WEIGHT: Double =
            c.driftHistoryWeight
        }
      case c: Configuration.Naive =>
        () => new hu.sztaki.drc.Naive {
          override protected val TAKE: Int =
            c.take
          override protected val HISTOGRAM_SCALE_BOUNDARY: Int =
            c.histogramSoftBoundary
          override protected val BACKOFF_FACTOR: Double =
            c.backoffFactor
          protected var HISTOGRAM_HARD_BOUNDARY =
            c.histogramHardBoundary
          override protected val HISTOGRAM_COMPACTION: Int =
            c.histogramCompaction
        }
      case c: Adaptive =>
        /**
          * @todo Continue! Make adaptive an abstract class.
          */
        () => new hu.sztaki.drc.Adaptive(c.take, c.backoffFactor) {
          HISTOGRAM_HARD_BOUNDARY = c.take * 2
          override protected val MIN_MEMORY: Int = c.take * 16
          override protected val DRIFT_BOUNDARY: Double = 1.0 / (2.0 * c.take.toDouble)
          override protected val DRIFT_HISTORY_WEIGHT: Double = 0.8
          override protected val GROW_FACTOR: Int = 2

          driftHistory = DRIFT_BOUNDARY
        }
    }

    var results = List.empty[(Configuration.Configuration, Long, Map[Any, Double], Option[Any])]

    var start: Long = 0l
    var end: Long = 0l
    var runtime: Long = 0l

    println(s"Starting test with configuration [${configuration.getClass.getName}].")

    def measure(h: => Map[Any, Double], m: => Any)(f: => Unit): Unit = {
      start = System.currentTimeMillis()

      f

      end = System.currentTimeMillis()

      runtime = end - start
      results = results :+ (configuration, runtime, h, m match {
        case a: drc.Sampling =>
          Some(a.widthHistory)
      })
    }

    def measureTyped[P](h: => Array[(P, Int)])(f: => Unit): Unit = {
      start = System.currentTimeMillis()

      f
      val result = h

      end = System.currentTimeMillis()

      runtime = end - start
      results = results :+ (configuration, runtime, result.toMap.map {
        case (k: P, v: Int) => (k, v.toDouble)
      }.asInstanceOf[Map[Any, Double]], Some(result.length))
    }

    /**
      * Actually running the test.
      */
    configuration match {
      case c: Configuration.Lossy =>
        val method = methodFactory().asInstanceOf[LossyCountingModel[T]]
        (1 to runs).foreach { _ =>
          measureTyped(method.computeOutput()) {
            (1 to rounds).foreach { _ =>
              method.process(data.iterator)
            }
          }
        }
      case c: Configuration.Conceptier =>
        val method = methodFactory().asInstanceOf[drc.Conceptier]
        (1 to runs).foreach { _ => measure(method.value, method) {
            (1 to rounds).foreach { _ =>
              data.iterator.foreach(t => method.add(t -> 1))
            }
          }
        }
      case c: Configuration.Adaptive =>
        val method = methodFactory().asInstanceOf[drc.Adaptive]
        (1 to runs).foreach { _ => measure(method.value, method) {
            (1 to rounds).foreach { _ =>
              data.iterator.foreach(t => method.add(t -> 1))
            }
          }
        }
      case c: Configuration.Naive =>
        val method = methodFactory().asInstanceOf[drc.Naive]
        (1 to runs).foreach { _ => measure(method.value, method) {
            (1 to rounds).foreach { _ =>
              data.iterator.foreach(t => method.add(t -> 1))
            }
          }
        }
    }

    results
  }

  def main(args: Array[String]): Unit = {
    /**
      * @note Dataset TS4K.
      */
    val timeseries4K = () => {
      println("Loading TS4K.")
      ("TS4K", CSVReader
        .open(args(0))(new CSVFormat {
          val delimiter: Char = '|'
          val quoteChar: Char = '"'
          val escapeChar: Char = '"'
          val lineTerminator: String = "\r\n"
          val quoting: Quoting = QUOTE_MINIMAL
          val treatEmptyLineAsNil: Boolean = false
        })
        .iterator
        .take(1000 * 1000 * 4)
        .flatMap {
          _.drop(6).headOption match {
            case Some(t) =>
              t.split(",")
            case None => Nil
          }
        }
        .toList)
    }

    /**
      * @note Dataset TSS10.
      */
    var i = 0
    val timeseriesSorted = () => {
      println("Loading TSS10.")
      ("TSS10", Source
        .fromFile(args(1))
        .getLines()
        .filter { _ =>
          i += 1
          i % 10 == 0
        }.map {
        _.drop(1).dropRight(1).split(",")(0)
        }
        .toList)
    }

    /**
      * @note Dataset TSR10.
      */
    i = 0
    val timeseriesRerverse = () => {
      println("Loading TSR10.")
      ("TSR10", Source
        .fromFile(args(1))
        .getLines()
        .filter { _ =>
          i += 1
          i % 10 == 0
        }.map {
        _.drop(1).dropRight(1).split(",")(0)
        }
        .toList
        .reverse)
    }


    /**
      * @note Dataset RND4K.
      */
    val random = () => {
      println("Loading RND4K.")
      ("RND4K", (1 to (1000 * 1000 * 4)).map {
        _ => RandomStringUtils.randomAlphabetic(4)
      }.toList)
    }


    /**
      * @note Dataset ZIPFXX.
      */
    val zipfians = {
      for (
        exponent <- 1.0 to 3.0 by 0.2;
        cardinality <- List(1000)
      ) yield {
        () => {
          println("Loading ZIPFXX.")
          var numberOfEvents = 0
          val zipf = new ZipfDistribution(1000 * 1000 * cardinality, exponent)
          var key = 0
          var myList = List.empty[String]
          val data = (s"ZIPF-$exponent", {
            while (numberOfEvents < 4000000) {
              val zipfian = zipf.sample()
              numberOfEvents += zipfian
              val randomString = key.toString
              key += 1
              myList = myList ++ List.fill(zipfian)(randomString)
            }
            scala.util.Random.shuffle(myList)
          })
          println(s"Size of data is [${data._2.size}].")
          data
        }
      }
    }

    val drifted = {
      for (
        exponent <- 1.0 to 3.0 by 0.2;
        cardinality <- List(1000)
      ) yield {
        () => {
          println("Loading ZIPFXX.")
          var numberOfEvents = 0
          val zipf = new ZipfDistribution(1000 * 1000 * cardinality, exponent)
          var key = 0
          (s"DRIFTED-ZIPF-$exponent",
            { (1 to 4).map{ _ => scala.util.Random.shuffle((1 to (1000 * 1000 * 4)).flatMap {
              _ =>
                if (numberOfEvents < 1000 * 1000) {
                  val zipfian = zipf.sample()
                  numberOfEvents += zipfian
                  val randomString = key.toString
                  key += 1
                  (1 to zipfian).map(_ => randomString).toIterator
                } else {
                  Iterator.empty
                }
            })
          }}.toList)
        }
      }
    }

    def show(record: (BigDecimal, Long, Configuration.Configuration, Int, Seq[(Any, BigDecimal)])): Unit = {
      println(s"Precision is [${record._1}].")
      println(s"Average runtime is [${record._2}].")
      println(s"Configuration is [${record._3.toString}].")
      println(s"Final histogram size is [${record._4}].")
    }

    val RUNS = 10
    val DROP = 3

    /**
      * Measurements.
      */
    for (
      K <- (25 to 500 by 25);
      generator <- zipfians ++ List(timeseries4K, timeseriesSorted, timeseriesRerverse, random) ++ drifted
    ) {
      println(s"K equals to [$K].")
      /**
        * Actually loading or generating data.
        */
      val (dataName, events) = generator()

      println("Data have been prepared.")

      val etalon = run(events, Configuration.Lossy(0.00001, 0.0), 1, 1)

      println("Etalon Lossy stats:")
      println(etalon.head._2)
      println(etalon.head._3.size)

      val etalonTopK = etalon.head._3.toSeq.sortBy(-_._2).take(K)
      val etalonNormedTopK = etalonTopK.map {
        case (k, v) => (k, BigDecimal(v) / BigDecimal(etalonTopK.map(_._2).sum))
      }

      etalonNormedTopK foreach {
        println
      }

      println {
        compact {
          render {
            ("etalon-top-k" -> etalonNormedTopK.map(p => (p._1.toString, p._2))) ~
            ("k" -> K) ~
            ("dataset" -> dataName) ~
            ("method" -> "etalon")
          }
        }
      }

      /**
        * Running Lossys.
        */
      for (frequency <- List(0.1, 0.001, 0.0001, 0.00001);
           error <- List(0, 0.01, 0.001, 0.0001)) {
        val lossy = run(events, Configuration.Lossy(frequency, error), 1, RUNS).drop(DROP)

        println("Current Lossy stats:")
        println(lossy.head._2)
        println(lossy.head._3.size)

        /**
          * @note Print Lossy results.
          */
        println {
          compact {
            render {
              ("method" -> "lossy") ~
                ("K" -> K) ~
                ("dataset" -> dataName) ~
                ("configuration" -> (
                  ("frequency" -> frequency) ~
                    ("error" -> error)
                  )) ~
                ("results" ->
                  lossy.map { r =>
                    ("top-k" -> r._3.map(pair => (pair._1.toString, pair._2)).toSeq.sortBy(-_._2).take(K)) ~
                    ("runtime" -> r._2) ~
                    ("width" -> r._4.get.asInstanceOf[Int])
                  }
                )
            }
          }
        }
      }


      /**
        * Running Conceptiers.
        */
      val backoffedConceptier = for (
        sizeBoundary <- (5 * K) to (75 * K) by (10 * K);
        driftHistoryWeight <- List(0.5);
        backoff <- List(1.2)
      ) yield {
        val set = run(events, Configuration.Conceptier(
          take = K,
          histogramSoftBoundary = sizeBoundary,
          backoffFactor = 1.05,
          histogramHardBoundary = sizeBoundary,
          histogramCompaction = sizeBoundary / 2,
          driftBoundary = 1.0 / (K.toDouble * 2.0),
          conceptSolidarity = 5,
          driftHistoryWeight = driftHistoryWeight), 1, RUNS).drop(DROP)

        println {
          compact {
            render {
              ("method" -> "conceptier") ~
                ("K" -> K) ~
                ("dataset" -> dataName) ~
                ("configuration" -> (
                  ("sizeBoundary" -> sizeBoundary) ~
                    ("driftHistoryWeight" -> driftHistoryWeight) ~
                    ("backoffFactor" -> backoff) ~
                    ("hardBoundary" -> sizeBoundary) ~
                    ("compaction" -> sizeBoundary / 2) ~
                    ("conceptSolidarity" -> 5) ~
                    ("driftBoundary" -> 1.0 / (K.toDouble * 2.0))
                  )) ~
                ("results" ->
                  set.map { r =>
                    ("top-k" -> r._3.map(pair => (pair._1.toString, pair._2)).toSeq.sortBy(-_._2).take(K)) ~
                      ("runtime" -> r._2) ~
                      ("widthHistory" -> r._4.get.asInstanceOf[List[Int]])
                  }
                )
            }
          }
        }

        val averageRuntime = set.map(_._2).sum / set.length

        println("\nStats for the following configuration:")
        println(set.head._1.toString)
        println(s"\nAverage runtime is [$averageRuntime].")

        var bestTopK: Seq[(Any, BigDecimal)] = null
        var bestPrecision: BigDecimal = BigDecimal(1.0)

        val precisions = set.map(_._3.toSeq.sortBy(-_._2).take(K)).map { conceptierTopK =>
          val conceptierNormedTopK = conceptierTopK.map {
            case (k, v) => (k, BigDecimal(v) / conceptierTopK.map(d => BigDecimal(d._2)).sum)
          }

          val precision = etalonNormedTopK.map {
            case (k, v) =>
              val value = conceptierNormedTopK.find(_._1 == k).map(_._2).getOrElse(BigDecimal(0.0))
              val diff = v - value
              diff * diff
          }.sum

          if (precision < bestPrecision) {
            bestPrecision = precision
            bestTopK = conceptierNormedTopK
          }

          precision
        }

        val precision = precisions.sum / BigDecimal(precisions.size)

        val widthHistory = set.map(_._4.get.asInstanceOf[List[Int]])
        val averageWidth = widthHistory.map(_.size).sum / widthHistory.size

        println(s"Precision is [$precision].\n")

        (precision, averageRuntime, set.head._1, set.head._3.size, bestTopK)
      }

      val topPrecision = backoffedConceptier.sortBy(_._1).take(10)
      val topRuntime = backoffedConceptier.sortBy(_._2).take(10)
      val badPrecision = backoffedConceptier.sortBy(-_._1).take(10)
      val badRuntime = backoffedConceptier.sortBy(-_._2).take(10)

      topPrecision.head._5.foreach {
        println(_)
      }

      println("\nTop precisions are:\n")

      topPrecision foreach show

      println("\nTop runtimes are:\n")

      topRuntime foreach show

      println("\nBad precisions are:\n")

      badPrecision foreach show

      println("\nBad runtimes are:\n")

      badRuntime foreach show


      /**
        * Running Naives.
        */
      for (
        sizeBoundary <- (5 * K) to (75 * K) by (10 * K);
        backoff <- List(1.2)
      ) yield {
        val set = run(events, Configuration.Naive(
          take = K,
          histogramSoftBoundary = sizeBoundary,
          backoffFactor = backoff,
          histogramHardBoundary = sizeBoundary,
          histogramCompaction = sizeBoundary / 2), 1, RUNS).drop(DROP)

        println {
          compact {
            render {
              ("method" -> "naiv") ~
                ("K" -> K) ~
                ("dataset" -> dataName) ~
                ("configuration" -> (
                  ("sizeBoundary" -> sizeBoundary) ~
                    ("backoffFactor" -> backoff) ~
                    ("hardBoundary" -> sizeBoundary) ~
                    ("compaction" -> sizeBoundary / 2)
                  )) ~
                ("results" ->
                  set.map { r =>
                    ("top-k" -> r._3.map(pair => (pair._1.toString, pair._2)).toSeq.sortBy(-_._2).take(K)) ~
                      ("runtime" -> r._2) ~
                      ("widthHistory" -> r._4.get.asInstanceOf[List[Int]])
                  }
                  )
            }
          }
        }
      }
    }
  }
}