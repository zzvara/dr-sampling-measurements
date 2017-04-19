package hu.sztaki.sm

import hu.sztaki.sm.Baseline.Configuration
import org.apache.spark.{SparkConf, SparkContext}

import org.json4s._
import org.json4s.native.JsonMethods._

import scalatags.Text.all._

object Really {
  def main(args: Array[String]): Unit = {
    @transient implicit lazy val formats = DefaultFormats

    val context = new SparkContext(
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("Whatever")
    )

    val sortedResults =
      context.textFile(
        "C:\\Users\\Ehnalis\\Projects\\dr-sampling-measurements\\measurements-2.txt", 50
      )
      .filter(_.startsWith("{"))
      .map {
        j =>
          parse(j)
      }
      .map {
        case j: JObject =>
          j \ "method" match {
            case JString("lossy") =>
              Lossy(
                (j \ "K").extract[Int],
                (j \ "dataset").extract[String],
                Configuration.Lossy(
                  (j \ "configuration" \ "frequency").extract[Double],
                  (j \ "configuration" \ "error").extract[Double]
                ),
                (j \ "results").asInstanceOf[JArray].arr.map {
                  r => Result(
                    (r \ "runtime").extract[Long],
                    List((r \ "width").extract[Int]),
                    (r \ "top-k").extract[List[(String, Double)]].toMap
                  )
                }
              )
            case JString("etalon") =>
              Etalon(
                (j \ "k").extract[Int],
                (j \ "dataset").extract[String],
                (j \ "etalon-top-k").extract[List[(String, Double)]].toMap
              )
            case JString("conceptier") =>
              Conceptier(
                (j \ "K").extract[Int],
                (j \ "dataset").extract[String],
                Configuration.Conceptier(
                  (j \ "K").extract[Int],
                  (j \ "configuration" \ "sizeBoundary").extract[Int],
                  (j \ "configuration" \ "backoffFactor").extract[Double],
                  (j \ "configuration" \ "hardBoundary").extract[Int],
                  (j \ "configuration" \ "compaction").extract[Int],
                  (j \ "configuration" \ "driftBoundary").extract[Double],
                  (j \ "configuration" \ "conceptSolidarity").extract[Int],
                  (j \ "configuration" \ "driftHistoryWeight").extract[Double]
                ),
                (j \ "results").asInstanceOf[JArray].arr.map {
                  r =>
                    val result = Result(
                    (r \ "runtime").extract[Long],
                    (r \ "widthHistory").extract[List[Int]],
                    (r \ "top-k").extract[List[(String, Double)]].toMap
                  )
                    result
                }
              )
            case JString("naiv") =>
              Naiv(
                (j \ "K").extract[Int],
                (j \ "dataset").extract[String],
                Configuration.Naive(
                  (j \ "K").extract[Int],
                  (j \ "configuration" \ "sizeBoundary").extract[Int],
                  (j \ "configuration" \ "backoffFactor").extract[Double],
                  (j \ "configuration" \ "hardBoundary").extract[Int],
                  (j \ "configuration" \ "compaction").extract[Int]
                ),
                (j \ "results").asInstanceOf[JArray].arr.map {
                  r =>
                    val result = Result(
                      (r \ "runtime").extract[Long],
                      (r \ "widthHistory").extract[List[Int]],
                      (r \ "top-k").extract[List[(String, Double)]].toMap
                    )
                    result
                }
              )
            case _ => null
          }
      }
      .filter(_ != null)
      .groupBy(_.asInstanceOf[Record].K)
      .map {
        groupForK =>
          (groupForK._1, groupForK._2.groupBy(_.asInstanceOf[Record].dataset)
            .map {
              groupForDataset =>
                println(s"grouped for [${groupForDataset._1}]")
                (
                  groupForDataset._1,
                  groupForDataset._2.groupBy(_.getClass.getName)
                )
            }
          )
      }
      .collect()

    context.stop()

    println("Data collected from Spark. Context stopped.")

    import java.io._
    val pw = new PrintWriter(new File("hello.html"))

    val header =
      """
        <html>
          <head></head>
          <body>
      """.stripMargin

    pw.write(header)

    sortedResults.foreach {
      groupForK =>
        pw.write(s"""<div><span>${groupForK._1}</span><div>""")
            groupForK._2.foreach {
              groupForDataset =>
                val datasetStuff = div(
                span(groupForDataset._1),
                table(
                  tr(
                    td("configuration"),
                    td("runtime max"),
                    td("runtime avg"),
                    td("runtime min"),
                    td("memory max"),
                    td("memory avg"),
                    td("memory min"),
                    td("error max"),
                    td("error avg"),
                    td("error min")
                  ) {
                    var etalon: Etalon = null
                    var normedEtalon: Seq[(String, BigDecimal)] = null
                    groupForDataset._2.map { groupForType =>
                      groupForType._2.map {
                        case l: Etalon =>
                          etalon = l
                          normedEtalon = etalon.histogram.toSeq.sortBy(-_._2)
                            .map(p => (p._1, BigDecimal(p._2)))
                          tr(

                          )
                        case l: Lossy =>
                          val errors = l.results.map(_.histogram).map {
                            h => Renderer.error(h, normedEtalon)
                          }
                          tr(
                            td(l.configuration.toString),
                            td(l.results.map(_.runtime).max),
                            td(l.results.map(_.runtime).sum / l.results.length),
                            td(l.results.map(_.runtime).min),
                            td(l.results.map(_.width.head).max),
                            td(l.results.map(_.width.head).sum / l.results.length),
                            td(l.results.map(_.width.head).min),
                            td(errors.max.toString),
                            td((errors.sum / errors.length).toString),
                            td(errors.min.toString)
                          )
                        case l: Conceptier =>
                          val errors = l.results.map(_.histogram).map {
                            h => Renderer.error(h, normedEtalon)
                          }
                          tr(
                            td(l.configuration.toString),
                            td(l.results.map(_.runtime).max),
                            td(l.results.map(_.runtime).sum / l.results.length),
                            td(l.results.map(_.runtime).min),
                            td(l.results.map(r => if (r.width.nonEmpty) r.width.max else -1).max),
                            td("?"),
                            td(l.results.map(r => if (r.width.nonEmpty) r.width.min else Int.MaxValue).min),
                            td(errors.max.toString),
                            td((errors.sum / errors.length).toString),
                            td(errors.min.toString)
                          )
                      }.toList
                    }.toList
                  }
                )
              )

              println("calculated group some group for dataset. writing out")
              pw.write(datasetStuff.render)
            }
        pw.write(s"""</div></div>""")
    }


    val footer =
      """
            </div></body></html>
      """.stripMargin

    pw.write(footer)

    pw.close
  }
}
