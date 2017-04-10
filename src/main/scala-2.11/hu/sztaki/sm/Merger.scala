package hu.sztaki.sm

import java.io.{File, FileOutputStream, PrintWriter}

import scala.io.Source

object Merger {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new FileOutputStream(
      new File("C:\\Users\\Ehnalis\\Downloads\\timeseries-sorted-increasing.csv"),
      true
    ))

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val files = getListOfFiles("C:\\Users\\Ehnalis\\Downloads\\timeseries-sorted-increasing")

    println(s"Found [${files.size}] number of files.")

    files.foreach {
      file => Source.fromFile(file).getLines().foreach {
        line => writer.append(line + "\n")
      }
    }

  }
}
