package com.wanghuanming

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  def suffixes(str: String): Iterator[String] = {
    str.tails.map(_ + '$')
  }
}
