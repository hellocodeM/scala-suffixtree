package com.wanghuanming

import com.abahgat.suffixtree.GeneralizedSuffixTree
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by ming on 16-11-17.
  */
@RunWith(classOf[JUnitRunner])
class UkkSuffixTreeTest extends FunSuite {

  test("simple usage") {
    val tree = new GeneralizedSuffixTree
    tree.put("hello", 0)

    val res: Iterable[Integer] = tree.search("llo").asScala.toList
    assert(res === List(0))

    tree.put("oolloe", 1)
    assert(tree.search("llo").asScala.toList === List(0, 1))
  }

  test("long string") {
    val SIZE = 10240
    val tree = new GeneralizedSuffixTree
    val str = Random.alphanumeric.take(SIZE).mkString
    tree.put(str, 0)

    val res = tree.search(str.substring(100, 110)).asScala.toList
    assert(res === List(0))

    assert(tree.search("123456789").isEmpty)
  }

}
