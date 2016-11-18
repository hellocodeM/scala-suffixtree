package com.wanghuanming

/**
  * Created by ming on 16-11-14.
  */
object Utils {

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

  def suffixesWithLabel(label: String, str: String): Set[String] = {
    str.tails.map(label + ":" + _ + "$").toSet
  }
}
