package com.wanghuanming

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  def readAllStringFromFile(filePath: String): mutable.ArrayBuffer[RangeSubString] = {
    val strs = new mutable.ArrayBuffer[RangeSubString]
    val dir = new File(filePath)
    for (file <- dir.listFiles) {
      val str: String = Utils.normalize(Source.fromFile(file))
      strs += RangeSubString(str + "$", file.getName)
    }
    strs
  }

  def normalize(input: BufferedSource): String = {
    input.getLines().mkString
  }

  def readAllStringFromFile(sc: SparkContext, filePath: String): mutable.ArrayBuffer[RangeSubString] = {
    val strs = new mutable.ArrayBuffer[RangeSubString]
    val strsRdd = sc.wholeTextFiles(filePath)
      .map(x => (x._1.substring(x._1.lastIndexOf("/") + 1), x._2.replace("\n", "")))
      .collect()

    for (file <- strsRdd)
      strs += RangeSubString(file._2, file._1)

    strs
  }

  def writeLeafInfoToLocalFile(filePath: String, suffixes: Array[String]): Unit = {
    val writer = new PrintWriter(new File(filePath))
    suffixes.foreach(writer.println)
    writer.close()
  }

  def writeLeafInfoToFile(filePath: String, suffixes: Array[String]): Unit = {
    val hdfsUrl = URI.create(filePath)
    val fs = FileSystem.get(URI.create("hdfs://" + hdfsUrl.getAuthority), new Configuration())

    val writer = new PrintWriter(fs.create(new Path(filePath)))
    suffixes.foreach(writer.println)
    writer.close
  }


  def getDistinctStr(strs: ArrayBuffer[RangeSubString]): String = {
    var res = ""
    for (str <- strs) {
      res = (res + str.mkString).distinct
    }
    res
  }

  def getUniqueTerminalSymbol(strs: String, terminalSymbol: Int): Char = {
    var res = terminalSymbol
    while (strs.contains(res.toChar)) {
      res += 1
    }
    res.toChar
  }

  /**
    * Compute suffixes fo the given string sequence.
    *
    * @return
    */
  def suffixes(strs: String*): Array[String] = {
    strs.zipWithIndex.flatMap { case (str: String, i: Int) =>
      suffixesWithLabel(i.toString, str)
    }.toArray.sorted
  }

  def suffixesWithLabel(label: String, str: String): Array[String] = {
    str.tails.filter(_.nonEmpty).map(label + ":" + _ + "$").toArray.sorted
  }
}
