package com.wanghuanming

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  /**
    * Compute suffixes fo the given string sequence.
    *
    * @return
    */

  def getDistinctStr(strs: ArrayBuffer[RangeSubString]): String = {
    var res = ""
    for (str <- strs) {
      res = (res + str.mkString.init).distinct
    }
    res
  }

  def suffixes(strs: String*): Array[String] = {
    strs.zipWithIndex.flatMap { case (str: String, i: Int) =>
      suffixesWithLabel(i.toString, str)
    }.toArray.sorted
  }

  def suffixesWithLabel(label: String, str: String): Array[String] = {
    str.tails.filter(_.nonEmpty).map(label + ":" + _ + "$").toArray.sorted
  }
}
