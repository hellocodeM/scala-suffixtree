package com.wanghuanming

/**
  * Created by ming on 16-11-12.
  * SubString of a long string could be represented as [start, end).
  * Label is used to identify same substring of two different string.
  */
class RangeSubString(source: String, start: Int, end: Int, val label: String) extends Serializable {

  def apply(idx: Int) = source(start + idx)

  def length = end - start

  def isEmpty = source.isEmpty || start >= end

  def nonEmpty = source.nonEmpty && start < end

  def substring(s: Int) = RangeSubString(source, start + s, end, label)

  def substring(s: Int, e: Int) = RangeSubString(source, start + s, start + e, label)

  def head: Char = source(start)

  def last: Char = source(end-1)
  /**
    * Common prefix but exclude terminal symbol, such as '$'.
    */
  def commonPrefix(rhs: RangeSubString): RangeSubString = {
    val len = length min rhs.length
    for (i <- 0 until len) {
      if (this (i) != rhs(i)) {
        return substring(0, i)
      }
    }
    this.substring(0, len)
  }

  def take(n: Int) = substring(0, n)

  def drop(n: Int) = substring(n, length)

  def mkString = source.substring(start, end)

  override def toString = mkString
}

object RangeSubString {

  def apply(s: String, start: Int, end: Int, label: String): RangeSubString = {
    new RangeSubString(s, start, end, label)
  }

  def apply(s: String, label: String = null): RangeSubString = {
    new RangeSubString(s, 0, s.length, label)
  }
}