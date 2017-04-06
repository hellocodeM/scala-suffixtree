package com.wanghuanming

/**
  * Created by ming on 16-11-12.
  * SubString of a long string could be represented as [start, end).
  * Label is used to identify same substring of two different string.
  */
class RangeSubString(source: String, start: Int, end: Int, val label: String, val index: Int) extends Serializable {

  def apply(idx: Int) = source(start + idx)

  def isEmpty = source.isEmpty || start >= end

  def nonEmpty = source.nonEmpty && start < end

  def substring(s: Int) = RangeSubString(source, start + s, end, label, index)

  def head: Char = source(start)

  def last: Char = source(end - 1)

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

  def length = end - start

  def substring(s: Int, e: Int) = {
    assert(s <= e)
    RangeSubString(source, start + s, start + e, label, index)
  }

  def take(n: Int) = substring(0, n)

  def drop(n: Int) = substring(n, length)

  override def toString = mkString //测试用

  //  override def toString = s"$source $start $end $label $index"

  def mkString = source.substring(start, end)
}

object RangeSubString {

  def apply(s: String, start: Int, end: Int, label: String, i: Int): RangeSubString = {
    new RangeSubString(s, start, end, label, i)
  }

  def apply(s: String, label: String = null): RangeSubString = {
    new RangeSubString(s, 0, s.length, label, 0)
  }
}