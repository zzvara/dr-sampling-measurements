package hu.sztaki.sm

import java.awt
import java.awt.{BasicStroke, Font, Paint, Shape, Stroke}
import java.io.Serializable

import com.jogamp.opengl.util.awt.TextRenderer
import com.jogamp.opengl.{GLCapabilities, GLProfile}
import com.jogamp.opengl.util.gl2.GLUT
import hu.sztaki.sm.Baseline.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jfree.chart.block.LineBorder
import org.jfree.chart.labels.BoxAndWhiskerToolTipGenerator
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.renderer.category.BoxAndWhiskerRenderer
import org.jfree.chart.renderer.xy.XYSplineRenderer
import org.jfree.chart.util.LogFormat
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart, axis}
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.{ApplicationFrame, RectangleEdge, RectangleInsets}
import org.json4s.DefaultFormats
import org.jzy3d.bridge.awt.FrameAWT
import org.jzy3d.chart.factories.{AWTChartComponentFactory, NewtChartComponentFactory}
import org.jzy3d.chart.graphs.GraphChart
import org.jzy3d.chart.{AWTChart, Chart, ChartLauncher}
import org.jzy3d.colors.colormaps.ColorMapRainbow
import org.jzy3d.colors.{Color, ColorMapper}
import org.jzy3d.maths
import org.jzy3d.maths.{Coord2d, Coord3d, Rectangle, Scale}
import org.jzy3d.plot3d.builder.concrete.OrthonormalGrid
import org.jzy3d.plot3d.builder.{Builder, Mapper}
import org.jzy3d.plot3d.primitives.ScatterMultiColor
import org.jzy3d.plot3d.primitives.axes.AxeBox
import org.jzy3d.plot3d.rendering.canvas.{CanvasAWT, Quality}
import org.jzy3d.plot3d.rendering.legends.colorbars.AWTColorbarLegend
import org.jzy3d.plot3d.text.DrawableTextWrapper
import org.jzy3d.plot3d.text.drawable.cells.{DrawableTextCell, TextCellRenderer}
import org.jzy3d.plot3d.text.renderers.jogl.{DefaultTextStyle, JOGLTextRenderer, ShadowedTextStyle}
import org.jzy3d.plot3d.text.renderers.{TextBillboardRenderer, TextBitmapRenderer}

import scala.collection.JavaConverters._

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

case class Spacey(
  override val method: String,
  override val K: Int,
  override val dataset: String,
  configuration: Configuration.Spacey,
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
    val textRenderer = new FunnyTextRenderer(
      new DefaultTextStyle(awt.Color.RED),
      new Font("Helvetica", Font.ITALIC, 20),
      true,
      new TextRenderer(new Font("Helvetica", Font.ITALIC, 20), false, false,
        new DefaultTextStyle(awt.Color.BLUE))
    )

/*
    val textRenderer2 =
      new TextCellRenderer(4, "DrawableTextCell(TextCellRenderer)",
        new Font("Serif", Font.PLAIN, 40))

    val drawable = new DrawableTextCell(textRenderer2, new Coord2d(0,14), new Coord2d(7,1))

    val whats = for(
      x <- (-10 to 10 by 10);
      y <- (-10 to 10 by 10);
      z <- (-10 to 10 by 10)
    ) yield { new DrawableTextWrapper(
        "5",
        new Coord3d(x,y,z), Color.BLACK, textRenderer
      )
    }

    val what = new DrawableTextWrapper(
      "DrawableTextWrapper(JOGLTextRenderer(DefaultTextStyle))",
      new Coord3d(0,0,0), Color.BLACK, textRenderer
    )

    val quality = Quality.Fastest
    quality.setSmoothColor(true)
    quality.setAnimated(true)
    quality.setSmoothPolygon(true)
    quality.setDepthActivated(false)
    val chart2 = new AWTChart(quality)
    chart2.add(drawable)
    whats.foreach { w =>
      chart2.add(w)
      //chart2.getScene.getGraph.add(w)
    }
/*
    val viewpoint = chart2.getViewPoint.mul(new Coord3d(0, 0, 0))
    chart2.setViewPoint(viewpoint)
    */
      chart2.addMouseCameraController()
      chart2.addKeyboardCameraController()
    chart2.getAWTView.setScale(new Scale(0.1, 4310.0))
    chart2.open("Jzy3d Demo", 500, 500)
    */

    val spark = new SparkContext(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Renderer")
    )

    val data = spark
      .textFile(arguments(0))
      .filter(_.startsWith("{"))
      .mapPartitions {
        _.map {
          l => {
            val json = parse(s"""$l""")
            implicit val format = new DefaultFormats with Serializable + new FieldSerializer[Result](
              FieldSerializer.renameTo("histogram", "top-k"),
              FieldSerializer.renameFrom("top-k", "histogram").orElse(
                FieldSerializer.renameFrom("widthHistory", "width")
              )
            ) with Serializable + new CustomSerializer[Result]((formats: Formats) => ({
              case o: JObject =>
                implicit val localFormat = DefaultFormats
                new Result(
                  (o \ "runtime").extract[Long],
                  (o \ "top-k").extract[List[Map[String, Double]]],
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
            })) with Serializable + new CustomSerializer[Configuration.Conceptier]((formats: Formats) => ({
              case o: JObject =>
                implicit val localFormat = DefaultFormats
                Configuration.Conceptier(
                  histogramHardBoundary = (o \ "sizeBoundary").extract[Int]
                )
            },{
              case r: Configuration.Conceptier =>
                implicit val localFormat = DefaultFormats
                Extraction.decompose(r)(localFormat)
            }))

            json \ "method" match {
              case JString("lossy") =>
                Lossy(
                  (json \ "method").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "K").extract[Int](format,
                    implicitly[Manifest[Int]]),
                  (json \ "dataset").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "configuration").extract[Configuration.Lossy](format,
                    implicitly[Manifest[Configuration.Lossy]]),
                  (json \ "results").extract[List[Result]](format,
                    implicitly[Manifest[List[Result]]])
                )
              case JString("spacey") =>
                Spacey(
                  (json \ "method").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "K").extract[Int](format,
                    implicitly[Manifest[Int]]),
                  (json \ "dataset").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "configuration").extract[Configuration.Spacey](format,
                    implicitly[Manifest[Configuration.Spacey]]),
                  (json \ "results").extract[List[Result]](format,
                    implicitly[Manifest[List[Result]]])
                )
              case JString("naiv") =>
                json.extract[Naiv](format,
                  implicitly[Manifest[Naiv]])
              case JString("conceptier") =>
                json.extract[Conceptier](format,
                  implicitly[Manifest[Conceptier]])
              case JString("etalon") =>
                Etalon(
                  (json \ "method").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "k").extract[Int](format,
                    implicitly[Manifest[Int]]),
                  (json \ "dataset").extract[String](format,
                    implicitly[Manifest[String]]),
                  (json \ "etalon-top-k").extract[List[Map[String, Double]]](format,
                    implicitly[Manifest[List[Map[String, Double]]]])
                    .fold(Map[String, Double]())(_ ++ _)
                )
            }
          }
        }
      }
      .persist(StorageLevel.DISK_ONLY)

    val etalons = data.filter {
      _.method == "etalon"
    }.map {
      _.asInstanceOf[Etalon]
    }
    .groupBy {
      _.K
    }.map {
      group =>
        (group._1, group._2.map( e =>
          (e.dataset, e.histogram.mapValues(BigDecimal(_)).toSeq.sortBy(_._1))).toMap
        )
    }.collect.toMap

    def visualizeSurface(
      data: List[Record],
      labels: (String, String, String),
      mappings: (Record => Double, Record => Double, Record => Double)): Unit = {

      val map = data
        .map {
          r =>
            (
              mappings._1(r),
              (mappings._2(r), mappings._3(r))
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
          map(x)(y)
        }
      }

      val kRange = new maths.Range(25, 525)
      val exponentRange = new maths.Range(1.0f, 3.0f)

      val surface = Builder.buildOrthonormal(new OrthonormalGrid(
        exponentRange, 9, kRange, 11), mapper
      )

      surface.setColorMapper(new ColorMapper(
        new ColorMapRainbow(), surface.getBounds.getZRange)
      )
      surface.setFaceDisplayed(true)
      surface.setWireframeDisplayed(false)
      surface.setWireframeColor(Color.BLACK)

      // Create a chart and add the surface
      val chart = new AWTChart(new NewtChartComponentFactory(), Quality.Advanced,
        Chart.DEFAULT_WINDOWING_TOOLKIT,
        new GLCapabilities(GLProfile.getMaximum(false)))
      chart.add(surface)
      chart.getAxeLayout.setXAxeLabel(labels._1)
      chart.getAxeLayout.setYAxeLabel(labels._2)
      chart.getAxeLayout.setZAxeLabel(labels._3)
      chart.getAWTView.getAxe.asInstanceOf[AxeBox].setTextRenderer(
        new TextBitmapRenderer() {
          fontHeight = 100
          font = GLUT.BITMAP_TIMES_ROMAN_24
        }
      )
      val viewpoint = chart.getViewPoint.mul(new Coord3d(0, 0, -300))
      chart.addMouseCameraController()
      chart.addKeyboardCameraController()
      chart.setViewPoint(viewpoint)
      chart.open("Jzy3d Demo", 1500, 1500)
    }


    def visualizeDelauney(
      data: List[Record],
      labels: (String, String, String),
      mappings: (Record => Double, Record => Double, Record => Double)): Unit = {

      val map = data
        .map {
          r =>
            (
              mappings._1(r),
              (mappings._2(r), mappings._3(r))
            )
        }
        .groupBy {
          _._1
        }
        .map {
          group =>
            (group._1, group._2.map(_._2).toMap)
        }

      val surface = Builder.buildDelaunay {
        map.flatMap {
          x =>
            x._2.map {
              pair =>
                new Coord3d(x._1, pair._2, pair._2)
            }
        }.toList.asJava
      }

      surface.setColorMapper(new ColorMapper(
        new ColorMapRainbow(), surface.getBounds.getZRange)
      )
      surface.setFaceDisplayed(true)
      surface.setWireframeDisplayed(false)
      surface.setWireframeColor(Color.BLACK)

      // Create a chart and add the surface
      val chart = new AWTChart(Quality.Advanced)
      chart.add(surface)
      chart.getAxeLayout.setXAxeLabel(labels._1)
      chart.getAxeLayout.setYAxeLabel(labels._2)
      chart.getAxeLayout.setZAxeLabel(labels._3)
      chart.getAWTView.getAxe.asInstanceOf[AxeBox].setTextRenderer(
        new TextBitmapRenderer() {
          fontHeight = 100
          font = GLUT.BITMAP_TIMES_ROMAN_24
        }
      )
      chart.addKeyboardCameraController()
      chart.open("Jzy3d Demo", 1200, 1200)
    }


    def visualizeScatter(
      data: List[Record],
      labels: (String, String, String),
      mappings: (Record => Double, Record => Double, Record => Double)): Unit = {

      val map = data
        .map {
          r =>
            (
              mappings._1(r),
              (mappings._2(r), mappings._3(r))
            )
        }
        .groupBy {
          _._1
        }
        .map {
          group =>
            (group._1, group._2.map(_._2).toMap)
        }

      val coords =
        map.flatMap {
          x =>
            x._2.map {
              pair =>
                new Coord3d(x._1, pair._2, pair._2)
            }
        }

      val scatter = new ScatterMultiColor(
        coords.toArray,
        new ColorMapper(
          new ColorMapRainbow(), Color.GREEN
        )
      )
      scatter.setWidth(20)

      // Create a chart and add the surface
      val chart = new AWTChart(Quality.Advanced)
      chart.add(scatter)
      chart.getAxeLayout.setXAxeLabel(labels._1)
      chart.getAxeLayout.setYAxeLabel(labels._2)
      chart.getAxeLayout.setZAxeLabel(labels._3)
      chart.getAWTView.getAxe.asInstanceOf[AxeBox].setTextRenderer(
        new TextBitmapRenderer() {
          fontHeight = 100
          font = GLUT.BITMAP_HELVETICA_18
        }
      )
      chart.addKeyboardCameraController()
      chart.getScene().add(scatter)
      ChartLauncher.openChart(chart)
      //chart.open("Jzy3d Demo", 1200, 1200)
    }

/*
    def visualize4DScatter(
      data: List[Record],
      labels: (String, String, String, String),
      mappings: (Record => Double,
                 Record => Double,
                 Record => Double,
                 Record => Double)): Unit = {

      val colorMap = new MyColorFunction()
      val myColorMap = new ColorMapper(colorMap, -1.0, 1.0)

      val colorData = data.map {
        r => mappings._4(r)
      }

      val min = colorData.min
      val max = colorData.max

      val map = data
        .map {
          r =>
            new Coord3d(mappings._1(r), mappings._2(r), mappings._3(r)) ->
            myColorMap.getColor(1 - (((mappings._4(r) - min) / max) * 2))
        }

      val scatter = new ScatterMultiColor(
        map.map(_._1).toArray,
        map.map(_._2).toArray,
        myColorMap
      )
      scatter.setWidth(20)
      val chart = new AWTChart(Quality.Advanced)
      scatter.setLegend( new AWTColorbarLegend(scatter,
        chart.getView().getAxe().getLayout().getZTickProvider(),
        chart.getView().getAxe().getLayout().getZTickRenderer()))
      scatter.setLegendDisplayed(true)

      // Create a chart and add the surface
      chart.add(scatter)
      chart.getAxeLayout.setXAxeLabel(labels._1)
      chart.getAxeLayout.setYAxeLabel(labels._2)
      chart.getAxeLayout.setZAxeLabel(labels._3)
      chart.getAWTView.getAxe.asInstanceOf[AxeBox].setTextRenderer(
        new TextBitmapRenderer() {
          fontHeight = 100
          font = GLUT.BITMAP_HELVETICA_18
        }
      )
      chart.addKeyboardCameraController()
      chart.getScene().add(scatter)
      ChartLauncher.openChart(chart)
      //chart.open("Jzy3d Demo", 1200, 1200)
    }
    */

    def showZipf(datasetPrefix: String): Unit = {

      /**
        * EXPONENT, K -> RUNTIME
        */
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "lossy"
        }.collect().toList,
        ("exponent", "k", "runtime"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
        )
      )

      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "spacey"
        }.collect().toList,
        ("exponent", "k", "runtime"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
        )
      )

      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "naiv"
        }.collect().toList,
        ("exponent", "k", "runtime"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
        )
      )

      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "conceptier"
        }.collect().toList,
        ("exponent", "k", "runtime"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
        )
      )


      /**
        * EXPONENT, K -> PRECISION
        */
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "lossy"
        }.collect().toList,
        ("exponent", "k", "precision"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            (r.results.map {
              result => error(result.histogram, etalon)
            }.sum / r.results.size).toDouble
          }
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "spacey"
        }.collect().toList,
        ("exponent", "k", "precision"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            (r.results.map {
              result => error(result.histogram, etalon)
            }.sum / r.results.size).toDouble
          }
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "naiv"
        }.collect().toList,
        ("exponent", "k", "precision"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            (r.results.map {
              result => error(result.histogram, etalon)
            }.sum / r.results.size).toDouble
          }
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "conceptier"
        }.collect().toList,
        ("exponent", "k", "precision"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            (r.results.map {
              result => error(result.histogram, etalon)
            }.sum / r.results.size).toDouble
          }
        )
      )


      /**
        * EXPONENT, K -> MEMORY
        */
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "lossy"
        }.collect().toList,
        ("exponent", "k", "memory"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.width.max).sum.toDouble / r.results.size.toDouble
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "spacey"
        }.collect().toList,
        ("exponent", "k", "memory"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.results.map(_.width.max).sum.toDouble / r.results.size.toDouble
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "naiv"
        }.collect().toList,
        ("exponent", "k", "memory"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => r.asInstanceOf[Naiv].configuration.histogramHardBoundary.toDouble
        )
      )
      visualizeSurface(
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "conceptier"
        }.collect().toList,
        ("exponent", "k", "memory"),
        (
          (r: Record) => r.dataset.split("-").last.toDouble,
          (r: Record) => r.K.toDouble,
          (r: Record) => {
            r.results.map {
              result => if(result.width.nonEmpty) {
                result.width.max / result.width.size.toDouble
              } else {
                result.histogram.size.toDouble
              }
            }.sum / r.results.size
          }
        )
      )

    }

    def show2D(
      datasetPrefix: String,
      yName: String,
      ySelector: (Record => List[Double], Record => List[Double], Record => List[Double], Record => List[Double]),
      yLog: Boolean = false,
      title: String = "whatever",
      withLossy: Boolean = true) = {
      val fontSize = 42

      val lossy = if (withLossy) {
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "lossy"
        }.collect().toList
      } else {
        List.empty[Record]
      }
      val spacey = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "spacey"
      }.collect().toList
      val naiv = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "naiv"
      }.collect().toList
      val conceptier = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "conceptier"
      }.collect().toList

      /*
      val lossySeries = new XYSeries("lossy")
      lossy.foreach {
        n => lossySeries.add(
          n.K.toDouble, n.results.map(_.runtime).sum / n.results.size
        )
      }

      val naivSeries = new XYSeries("naiv")
      naiv.foreach {
        n => naivSeries.add(
          n.K.toDouble, n.results.map(_.runtime).sum / n.results.size
        )
      }

      val conceptierSeries = new XYSeries("conceptier")
      conceptier.foreach {
        n => conceptierSeries.add(
          n.K.toDouble, n.results.map(_.runtime).sum / n.results.size
        )
      }

      val chartDataset = new XYSeriesCollection()
      chartDataset.addSeries(lossySeries)
      chartDataset.addSeries(naivSeries)
      chartDataset.addSeries(conceptierSeries)
      */


      val chartDataset = new DefaultBoxAndWhiskerCategoryDataset()
      if (withLossy) {
        lossy.foreach {
          n => chartDataset.add(
            ySelector._1(n).asJava, "lossy", n.K.toString
          )
        }
      }
      naiv.foreach {
        n => chartDataset.add(
          ySelector._2(n).asJava, "naiv", n.K.toString
        )
      }
      conceptier.foreach {
        n => chartDataset.add(
          ySelector._3(n).asJava, "drift resp", n.K.toString
        )
      }
      spacey.foreach {
        n => chartDataset.add(
          ySelector._4(n).asJava, "spacey", n.K.toString
        )
      }

      val xAxis = new axis.CategoryAxis("K")
      xAxis.setLabelFont(
        new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize)
      )
      xAxis.setTickLabelFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize - 5))
      val yAxis = if (yLog) {
        val a = new axis.LogAxis(yName)
        a.setBase(10)
        val format = new LogFormat(a.getBase(), "", "", true)
        a.setNumberFormatOverride(format)
        a
      } else {
        val a = new axis.NumberAxis(yName)
        a.setAutoRangeIncludesZero(false)
        a
      }
      yAxis.setLabelFont(
        new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize)
      )
      yAxis.setTickLabelFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize - 5))
/*
      val chart = ChartFactory.createXYLineChart(
        "whatever",
        "K",
        "runtime",
        chartDataset,
        PlotOrientation.HORIZONTAL,
        false,
        false,
        false
      )
      val panel = new ChartPanel(chart)
      panel.setPreferredSize(new java.awt.Dimension(1000, 500))
      panel.setMouseZoomable(true)
      val plot = chart.getXYPlot

      val renderer = new XYLineAndShapeRenderer()
      renderer.setSeriesPaint(0, awt.Color.RED)
      renderer.setSeriesPaint(1, awt.Color.GREEN)
      renderer.setSeriesPaint(2, awt.Color.BLUE)
      renderer.setSeriesStroke(0, new BasicStroke(4.0f))
      renderer.setSeriesStroke(1, new BasicStroke(3.0f))
      renderer.setSeriesStroke(2, new BasicStroke(2.0f))

      plot.setRenderer(renderer)
      */

      val renderer = new BoxAndWhiskerRenderer()
      renderer.setFillBox(true)
      renderer.getBaseLegendShape.getBounds.setSize(500, 500)
      renderer.setBaseLegendTextFont(new Font("CMU Serif Roman", Font.BOLD, fontSize))
      renderer.setToolTipGenerator(new BoxAndWhiskerToolTipGenerator())
      renderer.setBaseItemLabelFont(new Font("CMU Serif Roman", Font.BOLD, fontSize))
      renderer.setItemMargin(0.03)

      val plot = new CategoryPlot(chartDataset, xAxis, yAxis, renderer)
      plot.setBackgroundPaint(awt.Color.WHITE)

      val chart = new JFreeChart(
        title,
        new Font("CMU Serif Roman", Font.BOLD, fontSize),
        plot,
        true
      )
      chart.setBackgroundPaint(awt.Color.WHITE)

      val panel = new ChartPanel(chart)
      panel.setPreferredSize(new java.awt.Dimension(1000, 500))
      panel.setBackground(awt.Color.WHITE)

      val applicationFrame = new ApplicationFrame(title)

      applicationFrame.setContentPane(panel)
      applicationFrame.setBackground(awt.Color.WHITE)

      applicationFrame.show()
    }


    def show2DSpline(
                datasetPrefix: String,
                yName: String,
                ySelector: (Record => List[Double], Record => List[Double], Record => List[Double], Record => List[Double]),
                yLog: Boolean = false,
                title: String = "whatever",
                withLossy: Boolean = true) = {
      val fontSize = 42
      val lossy = if (withLossy) {
        data.filter {
          r => r.dataset.startsWith(datasetPrefix) && r.method == "lossy"
        }.collect().toList
      } else {
        List.empty[Record]
      }
      val naiv = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "naiv"
      }.collect().toList
      val conceptier = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "conceptier"
      }.collect().toList
      val spacey = data.filter {
        r => r.dataset.startsWith(datasetPrefix) && r.method == "spacey"
      }.collect().toList

      val chartDataset = new XYSeriesCollection()
      if (withLossy) {
        val lossyData = new XYSeries("lossy")
        lossy.groupBy(_.K).foreach {
          group =>
            val recordsMod = group._2.map(n => ySelector._1(n).sum / ySelector._1(n).size)
            lossyData.add(
              recordsMod.sum / recordsMod.size, group._1.toDouble
            )
        }
        chartDataset.addSeries(lossyData)
      }
      val naivData = new XYSeries("naiv")
      naiv.groupBy(_.K).foreach {
        group =>
          val recordsMod = group._2.map(n => ySelector._2(n).sum / ySelector._2(n).size)
          naivData.add(
            recordsMod.sum / recordsMod.size, group._1.toDouble
          )
      }
      chartDataset.addSeries(naivData)
      val conceptierData = new XYSeries("drift resp")
      conceptier.groupBy(_.K).foreach {
        group =>
          val recordsMod = group._2.map(n => ySelector._3(n).sum / ySelector._3(n).size)
          conceptierData.add(
            recordsMod.sum / recordsMod.size, group._1.toDouble
          )
      }
      chartDataset.addSeries(conceptierData)
      val spaceyData = new XYSeries("spacey")
      spacey.groupBy(_.K).foreach {
        group =>
          val recordsMod = group._2.map(n => ySelector._4(n).sum / ySelector._4(n).size)
          spaceyData.add(
            recordsMod.sum / recordsMod.size, group._1.toDouble
          )
      }
      chartDataset.addSeries(spaceyData)

      val renderer = new XYSplineRenderer()
      renderer.setBaseLegendTextFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize))
      renderer.setBaseItemLabelFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize))
      renderer.setBaseStroke(new BasicStroke(6.0f))
      renderer.setBaseFillPaint(awt.Color.WHITE)
      renderer.setSeriesFillPaint(0, awt.Color.WHITE)
      renderer.setSeriesFillPaint(1, awt.Color.WHITE)
      renderer.setSeriesFillPaint(2, awt.Color.WHITE)
      renderer.setSeriesStroke(0, new BasicStroke(4.0f))
      renderer.setSeriesStroke(1, new BasicStroke(4.0f))
      renderer.setSeriesStroke(2, new BasicStroke(4.0f))

      val chart = ChartFactory.createXYLineChart(
        null, null, null, chartDataset, PlotOrientation.HORIZONTAL, true, true, false)

      val yAxis = if (yLog) {
        val a = new axis.LogAxis(yName)
        a.setBase(10)
        val format = new LogFormat(a.getBase(), "", "", true)
        a.setNumberFormatOverride(format)
        a
      } else {
        val a = new axis.NumberAxis(yName)
        a.setAutoRangeIncludesZero(false)
        a
      }
      yAxis.setLabelFont(
        new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize)
      )
      yAxis.setTickLabelFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize))

      val xAxis = new axis.NumberAxis("K")
      xAxis.setLabelFont(
        new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize)
      )
      xAxis.setTickLabelFont(new Font("CMU Serif Roman", Font.ROMAN_BASELINE, fontSize))

      chart.getXYPlot().setDomainAxis(yAxis)
      chart.getXYPlot().setRangeAxis(xAxis)
      chart.setBackgroundPaint(awt.Color.WHITE)
      chart.getXYPlot().setBackgroundPaint(awt.Color.WHITE)

      chart.getXYPlot().setRenderer(renderer)

      val panel = new ChartPanel(chart)
      panel.setPreferredSize(new java.awt.Dimension(1000, 500))
      panel.setBackground(awt.Color.WHITE)

      val applicationFrame = new ApplicationFrame(title)

      applicationFrame.setContentPane(panel)
      applicationFrame.setBackground(awt.Color.WHITE)

      applicationFrame.show()
    }

    // showZipf("ZIPF")
    // showZipf("DRIFTED-ZIPF")

    show2D(
      "TS4K",
      "runtime (milliseconds)",
      (
        (r: Record) => r.results.map(_.runtime.toDouble),
        (r: Record) => r.results.map(_.runtime.toDouble),
        (r: Record) => r.results.map(_.runtime.toDouble),
        (r: Record) => r.results.map(_.runtime.toDouble)
      ),
      title = "TS dataset"
    )

    show2D(
      "TS4K",
      "error (log scale, base 10)",
      (
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          r.results.map {
            result => error(result.histogram, etalon).toDouble
          }
        },
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          r.results.map {
            result => error(result.histogram, etalon).toDouble
          }
        },
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          r.results.map {
            result => error(result.histogram, etalon).toDouble
          }
        },
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          r.results.map {
            result => error(result.histogram, etalon).toDouble
          }
        }
      ),
      yLog = true,
      withLossy = false,
      title = "TS dataset"
    )

    show2DSpline(
      "TS4K",
      "memory (log, base 10)",
      (
        (r: Record) => r.results.map(_.width.max.toDouble),
        (r: Record) => List.fill(r.results.size)(r.asInstanceOf[Naiv].configuration.histogramHardBoundary.toDouble),
        (r: Record) => r.results.map {
          result => if(result.width.nonEmpty) {
            result.width.max / result.width.size.toDouble
          } else {
            result.histogram.size.toDouble
          }
        },
        (r: Record) => r.results.map(_.width.max.toDouble)
      ),
      yLog = true,
      title = "TS dataset"
    )
/*
    for (dataset <- List("RND4K")) {
      show2D(
        dataset,
        "runtime",
        (
          (r: Record) => r.results.map(_.runtime.toDouble),
          (r: Record) => r.results.map(_.runtime.toDouble),
          (r: Record) => r.results.map(_.runtime.toDouble)
        ),
        yLog = true
      )

      show2D(
        dataset,
        "precision",
        (
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            r.results.map {
              result => error(result.histogram, etalon).toDouble
            }
          },
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            r.results.map {
              result => error(result.histogram, etalon).toDouble
            }
          },
          (r: Record) => {
            val etalon = etalons(r.K)(r.dataset)
            r.results.map {
              result => error(result.histogram, etalon).toDouble
            }
          }
        ),
        yLog = true
      )

      show2D(
        dataset,
        "memory",
        (
          (r: Record) => r.results.map(_.width.max.toDouble),
          (r: Record) => List.fill(r.results.size)(r.asInstanceOf[Naiv].configuration.histogramHardBoundary.toDouble),
          (r: Record) => r.results.map {
            result =>
              if (result.width.nonEmpty) {
                result.width.max / result.width.size.toDouble
              } else {
                result.histogram.size.toDouble
              }
          }
        ),
        yLog = true
      )
    }

    visualizeDelauney(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "naiv"
      },
      ("exponent", "memory", "runtime"),
      (
        (r: Record) =>
          r.dataset.split("-").drop(1).head.toDouble * 100,
        (r: Record) => r.asInstanceOf[Naiv].configuration.histogramHardBoundary.toDouble,
        (r: Record) =>
          r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
      )
    )

    visualizeDelauney(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "conceptier"
      },
      ("exponent", "memory", "runtime"),
      (
        (r: Record) => r.dataset.split("-").drop(1).head.toDouble,
        (r: Record) => {
          r.results.map {
            result => if(result.width.nonEmpty) {
              result.width.max / result.width.size.toDouble
            } else {
              result.histogram.size.toDouble
            }
          }.sum / r.results.size
        },
        (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
      )
    )

    visualizeDelauney(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "lossy"
      },
      ("exponent", "memory", "runtime"),
      (
        (r: Record) => r.dataset.split("-").drop(1).head.toDouble,
        (r: Record) => r.results.map(_.width.max).sum.toDouble / r.results.size.toDouble,
        (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
      )
    )

    visualizeScatter(
      data.filter {
        r => r.dataset.startsWith("TS4K") && r.method == "conceptier"
      }.collect().toList,
      ("sizeBoundary", "k", "precision"),
      (
        (r: Record) => r.asInstanceOf[Conceptier].configuration.histogramHardBoundary.toDouble,
        (r: Record) => r.K.toDouble,
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          (r.results.map {
            result => error(result.histogram, etalon)
          }.sum / r.results.size).toDouble
        }
      )
    )

    visualizeScatter(
      data.filter {
        r => r.dataset.startsWith("TS4K") && r.method == "conceptier"
      }.collect().toList,
      ("sizeBoundary", "k", "runtime"),
      (
        (r: Record) => r.asInstanceOf[Conceptier].configuration.histogramHardBoundary.toDouble,
        (r: Record) => r.K.toDouble,
        (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
      )
    )
*/
/*
    visualizeScatter(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "conceptier"
      }.collect().toList,
      ("sizeBoundary", "k", "precision"),
      (
        (r: Record) => r.asInstanceOf[Conceptier].configuration.histogramHardBoundary.toDouble,
        (r: Record) => r.K.toDouble,
        (r: Record) => {
          val etalon = etalons(r.K)(r.dataset)
          (r.results.map {
            result => error(result.histogram, etalon)
          }.sum / r.results.size).toDouble
        }
      )
    )


    visualizeScatter(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "conceptier"
      }.collect().toList,
      ("sizeBoundary", "k", "memory"),
      (
        (r: Record) => r.asInstanceOf[Conceptier].configuration.histogramHardBoundary.toDouble,
        (r: Record) => r.K.toDouble,
        (r: Record) => {
          r.results.map {
            result => if(result.width.nonEmpty) {
              result.width.max / result.width.size.toDouble
            } else {
              result.histogram.size.toDouble
            }
          }.sum / r.results.size
        }
      )
    )


    visualizeScatter(
      data.filter {
        r => r.dataset.startsWith("ZIPF") && r.method == "conceptier"
      }.collect().toList,
      ("sizeBoundary", "k", "runtime"),
      (
        (r: Record) => r.asInstanceOf[Conceptier].configuration.histogramHardBoundary.toDouble,
        (r: Record) => r.K.toDouble,
        (r: Record) => r.results.map(_.runtime).sum.toDouble / r.results.size.toDouble
      )
    )*/

    println("Visualized all")



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