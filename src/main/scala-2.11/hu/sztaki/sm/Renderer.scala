package hu.sztaki.sm

import com.jogamp.opengl.util.gl2.GLUT
import hu.sztaki.sm.Baseline.Configuration
import org.json4s.DefaultFormats
import org.jzy3d.chart.AWTChart
import org.jzy3d.colors.colormaps.ColorMapRainbow
import org.jzy3d.colors.{Color, ColorMapper}
import org.jzy3d.maths
import org.jzy3d.maths.Coord3d
import org.jzy3d.maths.algorithms.interpolation.algorithms.BernsteinInterpolator
import org.jzy3d.plot3d.builder.concrete.OrthonormalGrid
import org.jzy3d.plot3d.builder.{Builder, Mapper}
import org.jzy3d.plot3d.primitives.axes.AxeBox
import org.jzy3d.plot3d.rendering.canvas.Quality
import org.jzy3d.plot3d.text.renderers.TextBitmapRenderer

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.io.Source

case class Conceptier(
  override val method: String,
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Conceptier,
  override val results: List[Result])
extends Record(method, K, dataset, results)

case class Naiv(
  override val method: String,
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Naive,
  override val results: List[Result])
extends Record(method, K, dataset, results)

case class Lossy(
  override val method: String,
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Lossy,
  override val results: List[Result])
extends Record(method, K, dataset, results)

case class Etalon(
  override val method: String,
  override val K: Int,
  override val dataset: String,
  histogram: Map[String, Double])
extends Record(method, K, dataset, List.empty)

case class Result(
  runtime: Long,
  width: List[Int],
  histogram: Map[String, Double]) {
  def this(runtime: Long, histogram: List[Map[String, Double]], width: List[Int]) = {
    this(runtime, width, histogram.fold(Map[String, Double]())(_ ++ _))
  }
  def this(runtime: Long, histogram: Map[String, Double], width: Int) = {
    this(runtime, List(width), histogram)
  }
  def this(runtime: Long, histogram: List[Map[String, Double]], width: Int) = {
    this(runtime, List(width), histogram.fold(Map[String, Double]())(_ ++ _))
  }
}

class Record(val method: String, val K: Int, val dataset: String, val results: List[Result]) extends Serializable

import org.json4s._
import org.json4s.native.JsonMethods._

object Renderer {
  def main(arguments: Array[String]) = {
    implicit val format = DefaultFormats + new FieldSerializer[Result](
      FieldSerializer.renameTo("histogram", "top-k"),
      FieldSerializer.renameFrom("top-k", "histogram").orElse(
        FieldSerializer.renameFrom("widthHistory", "width")
      )
    ) + new CustomSerializer[Result]((formats: Formats) => ({
      case o: JObject =>
        implicit val localFormat = DefaultFormats
        new Result(
          (o \ "runtime").extract[Long],
          (o \ "histogram").extract[List[Map[String, Double]]],
          (o \ "width") match {
            case JInt(i) => List[Int](i.toInt)
            case a: JArray => a.extract[List[Int]]
            case JNothing =>
              (o \ "widthHistory") match {
                case JInt(i) => List[Int](i.toInt)
                case a: JArray => a.extract[List[Int]]
              }
          }
        )
    },{
      case r: Result =>
        implicit val localFormat = DefaultFormats
        Extraction.decompose(r)(localFormat)
    }))

    val data = Source
      .fromFile(arguments(0))
      .getLines()
      .filter(_.startsWith("{"))
      .map {
        l => {
          val json = parse(s"""$l""")

          json \ "method" match {
            case JString("lossy") =>
              Lossy(
                (json \ "method").extract[String],
                (json \ "K").extract[Int],
                (json \ "dataset").extract[String],
                (json \ "configuration").extract[Configuration.Lossy],
                (json \ "results").extract[List[Result]]
              )
            case JString("naiv") =>
              json.extract[Naiv]
            case JString("conceptier") =>
              json.extract[Conceptier]
            case JString("etalon") =>
              Etalon(
                (json \ "method").extract[String],
                (json \ "k").extract[Int],
                (json \ "dataset").extract[String],
                (json \ "etalon-top-k").extract[List[Map[String, Double]]]
                  .fold(Map[String, Double]())(_ ++ _)
              )
          }
        }
      }
      .toList

    val map = data.filter {
      r => r.dataset.startsWith("ZIPF") && r.method == "naiv"
    }.map {
      r =>
        (
          r.dataset.split("-").drop(1).head.toDouble,
          (
            r.K.toDouble,
            r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
          )
        )
    }
    .groupBy {
      _._1
    }
    .map {
      group =>
        (group._1, group._2.map(_._2).toMap)
    }

    val mapper = new Mapper {
      override def f(x: Double, y: Double) = {
        map(x / 4)(y)
      }
    }

    val kRange = new maths.Range(25, 525)
    val exponentRange = new maths.Range(1 * 4, 3 * 4)

    val surface = Builder.buildOrthonormal(new OrthonormalGrid(
      exponentRange, 9, kRange, 11), mapper
    )

    /*
    val surface = Builder.buildDelaunay {
      val dataPoints = (1.0 to 3.0 by 0.25).flatMap {
        x => (25 to 525 by 50).map {
          y => new Coord3d(x, y.toDouble, map(x)(y.toDouble))
        }
      }.toList.asJava
      new BernsteinInterpolator().interpolate(dataPoints, 3)
    }
    */

    surface.setColorMapper(new ColorMapper(
      new ColorMapRainbow(), surface.getBounds.getZRange)
    )
    surface.setFaceDisplayed(true)
    surface.setWireframeDisplayed(false)
    surface.setWireframeColor(Color.BLACK)

    // Create a chart and add the surface
    val chart = new AWTChart(Quality.Advanced)
    chart.add(surface)
    chart.getAxeLayout.setXAxeLabel("exponent")
    chart.getAxeLayout.setYAxeLabel("k")
    chart.getAxeLayout.setZAxeLabel("runtime")
    chart.getAWTView.getAxe.asInstanceOf[AxeBox].setTextRenderer(
      new TextBitmapRenderer() {
        fontHeight = 100
        font = GLUT.BITMAP_HELVETICA_18
      }
    )
    chart.open("Jzy3d Demo", 600, 600)

    ()
  }

  def error(histogram: Map[String, Double], reference: Seq[(AnyRef, BigDecimal)]): BigDecimal = {
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