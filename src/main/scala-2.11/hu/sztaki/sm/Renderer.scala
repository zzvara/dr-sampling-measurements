package hu.sztaki.sm

import com.quantifind.charts.highcharts.Highchart.traversableToTraversableSeries
import hu.sztaki.sm.Baseline.Configuration
import org.scalajs
import org.scalajs.dom
import org.scalajs.dom.ext.Ajax

import scala.concurrent.ExecutionContext.Implicits.global
import scala.scalajs.js.{JSApp, JSON, isUndefined}
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}
import scalatags.Text.all._

@JSExport
case class Conceptier(
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Conceptier,
  override val results: List[Result])
extends Record(K, dataset, results)

@JSExport
case class Naiv(
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Naive,
  override val results: List[Result])
extends Record(K, dataset, results)

@JSExport
case class Lossy(
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Lossy,
  override val results: List[Result])
extends Record(K, dataset, results)

@JSExport
case class Etalon(
  override val K: Int,
  override val dataset: String,
  histogram: Map[String, Double])
extends Record(K, dataset, List.empty)

@JSExport
case class Result(
  runtime: Long,
  width: List[Int],
  histogram: Map[String, Double])

class Record(val K: Int, val dataset: String, val results: List[Result]) extends Serializable

@JSExport
object Renderer extends JSApp {
  @JSExport
  def main() = {
    def parseAndRender(log: String): Unit = {
      val rendered = log.lines.filter(_.startsWith("{")).toList

      dom.console.log(s"Total of [${rendered.length}] JSON rows found.")

      val sortedResults = rendered.map { input =>
        val json = input.replace("&quot;", "\"")
        val parsed = JSON.parse(json)

        val method = parsed.method
        if (!isUndefined(parsed.selectDynamic("etalon-top-k"))) {
          Etalon(
            parsed.k.toString.toInt,
            parsed.dataset.toString,
            parsed
              .selectDynamic("etalon-top-k")
              .asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].flatMap {
                entry => entry.asInstanceOf[scala.scalajs.js.Dictionary[Double]].toMap
              }.toMap
          )
        } else if (parsed.method.asInstanceOf[String] == "lossy") {
          Lossy(
            parsed.K.toString.toInt,
            parsed.dataset.toString,
            Configuration.Lossy(
              parsed.configuration.frequency.toString.toDouble,
              parsed.configuration.error.toString.toDouble
            ),
            parsed.results.asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].map {
              result => Result(
                result.runtime.toString.toLong,
                List(result.width.toString.toInt),
                result.selectDynamic("top-k").asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].flatMap {
                  entry => entry.asInstanceOf[scala.scalajs.js.Dictionary[Double]].toMap
                }.toMap
              )
            }.toList
          )
        } else if (parsed.method.asInstanceOf[String] == "conceptier") {
          Conceptier(
            parsed.K.toString.toInt,
            parsed.dataset.toString,
            Configuration.Conceptier(
              parsed.K.toString.toInt,
              parsed.configuration.sizeBoundary.toString.toInt,
              parsed.configuration.backoffFactor.toString.toDouble,
              parsed.configuration.hardBoundary.toString.toInt,
              parsed.configuration.compaction.toString.toInt,
              parsed.configuration.driftBoundary.toString.toDouble,
              parsed.configuration.conceptSolidarity.toString.toInt,
                parsed.configuration.driftHistoryWeight.toString.toDouble
            ),
            parsed.results.asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].map {
              result => Result(
                result.runtime.toString.toLong,
                result.widthHistory.asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].map(_.toString.toInt).toList,
                result.selectDynamic("top-k").asInstanceOf[scala.scalajs.js.Array[scala.scalajs.js.Dynamic]].flatMap {
                  entry => entry.asInstanceOf[scala.scalajs.js.Dictionary[Double]].toMap
                }.toMap
              )
            }.toList
          )
        }
      }
      .groupBy(_.asInstanceOf[Record].K)
        .map {
          groupForK =>
            (groupForK._1, groupForK._2.groupBy(_.asInstanceOf[Record].dataset)
              .map {
                groupForDataset =>
                  (
                    groupForDataset._1,
                    groupForDataset._2.groupBy(_.getClass.getName)
                  )
              }
            )
        }

      val finalTable = sortedResults.map {
        groupForK =>
          div(
            span(groupForK._1),
            div(
              groupForK._2.map { groupForDataset =>
                div(
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
                            h => error(h, normedEtalon)
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
                      }
                    }.toList
                  })
                )
              }.toList
            )
          )
      }.map(_.render).mkString(" ")

      dom.console.log("Rendered.")

      dom.document.getElementById("Content").innerHTML = finalTable
    }

    Ajax.get(
      url = "/dr-sampling-measurements/measurements-1.txt",
      headers = Map("Content-Type" -> "text; charset=UTF-8"),
      responseType = "text"
    )
    .onComplete {
      case Success(success) =>
        dom.console.log("Loaded.")
        dom.console.log(success.responseText.size)
        parseAndRender(success.responseText)
      case Failure(failure) =>
        dom.console.log(failure.getCause.getMessage)
        scalajs.dom.window.alert(failure.getCause.getMessage)
    }
  }


  def error(histogram: Map[String, Double], reference: Seq[(Any, BigDecimal)]): BigDecimal = {
    val currentTopK =
      histogram.toSeq.sortBy(-_._2).map(p => (p._1, BigDecimal(p._2)))

    val normedTopK = currentTopK.map {
      case (k, v) => (k, v / currentTopK.map(d => d._2).sum)
    }

    reference.map {
      case (k, v) =>
        val value = normedTopK.find(_._1 == k).map(_._2).getOrElse(BigDecimal(0.0))
        val diff = v - value
        diff * diff
    }.sum
  }
}