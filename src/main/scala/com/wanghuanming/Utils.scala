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
  def suffixes(strs: String*): Set[String] = {
    strs.zipWithIndex.flatMap { case (str: String, i: Int) =>
      str.tails.map(_ + "$" + i)
    }.toSet
  }

  def suffixesWithTerminal(str: String, terminal: String): Set[String] = {
    str.tails.map(_ + terminal).toSet
  }
}
