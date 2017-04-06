package com.wanghuanming

import org.scalatest.FunSuite

/**
  * Created by ming on 17-4-6.
  */
class RangeSubStringTest extends FunSuite {

  test("commonPrefix") {
    val cases = Iterable(
      ("hello", "helle", "hell"),
      ("a", "b", ""),
      ("abcd", "abc", "abc"),
      ("a", "abcd", "a"),
      ("abc", "abc", "abc")
    )
    for (c <- cases) {
      val s1 = RangeSubString(c._1)
      val s2 = RangeSubString(c._2)
      val common = s1.commonPrefix(s2)
      assert(c._3 === common.toString)
    }
  }

}
