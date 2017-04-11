package com.wanghuanming.suffixtree

/**
  * Created by ming on 16-11-12.
  * SubString of a long string could be represented as [start, end).
  * Label is used to identify same substring of two different string.
  */
case class RangeSubString(source: String, start: Int, end: Int, label: String, index: Int) extends Serializable {

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

  def take(n: Int) = substring(0, Math.min(this.length, n))

  def drop(n: Int) = substring(Math.min(this.length, n), length)

  override def toString = mkString

  def mkString = source.substring(start, end)
}

object RangeSubString {

  def apply(s: String, label: String = null): RangeSubString = {
    new RangeSubString(s, 0, s.length, label, 0)
  }

  def apply(s: String, label: String, index: Int): RangeSubString = {
    new RangeSubString(s, 0, s.length, label, index)
  }

}