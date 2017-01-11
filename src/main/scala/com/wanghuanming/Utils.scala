package com.wanghuanming

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
  * Created by ming on 16-11-14.
  */
object Utils {



  def normalize(input: BufferedSource): String = {
    input.getLines().mkString
  }

  def readAllStringFromFile(filePath: String): mutable.ArrayBuffer[RangeSubString] = {
    val strs = new mutable.ArrayBuffer[RangeSubString]
    val dir = new File(filePath)
    for (file <- dir.listFiles) {
      val str:String = Utils.normalize(Source.fromFile(file))
      strs += RangeSubString(str + "$", file.getName)
    }
    strs
  }

  def writeLeafInfoToFile(filePath: String, suffixes: Array[String]): Unit = {
    val writer = new PrintWriter(new File(filePath))
    suffixes.foreach(writer.println)
    writer.close()
  }


  def getDistinctStr(strs: ArrayBuffer[RangeSubString]): String = {
    var res = ""
    for (str <- strs) {
      res = (res + str.mkString.init).distinct
    }
    res
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
