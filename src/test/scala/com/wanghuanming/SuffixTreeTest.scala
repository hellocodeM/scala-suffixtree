package com.wanghuanming

import com.abahgat.suffixtree.GeneralizedSuffixTree
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.util.Random
/**
  * Created by ming on 16-11-13.
  */
@RunWith(classOf[JUnitRunner])
class SuffixTreeTest extends FunSuite {

  test("abahgat implementation") {
    val tree = new GeneralizedSuffixTree
    tree.put("hello", 0)

    val res: Iterable[Integer] = tree.search("llo").asScala.toList
    assert(res === List(0))
  }

  test("abahgat implementation with very long string") {
    val tree = new GeneralizedSuffixTree
    val str = Random.alphanumeric.take(1024).mkString
    tree.put(str, 0)

    val res = tree.search(str.substring(100, 110)).asScala.toList
    assert(res === List(0))
  }

  test("insertSuffix") {
    val tree = new SuffixTree
    tree.insertSuffix(SubString("hello", 0, 5, "1.txt"))

  }

  test("insertSuffix with prefix") {
    val tree = new SuffixTree
    tree.insertSuffix(SubString("LLO$", 0, 4, "1.txt"))
    tree.insertSuffix(SubString("LO$", 0, 3, "2.txt"))

    val res = tree.strings
    assert(res sameElements Array("LLO$", "LO$"))
  }

  test("insert") {
    val tree = new SuffixTree
    tree.insert("hello", "1.txt")

    val expected = "hello".tails.map(_ + '$').toSet
    assert(tree.strings.toSet === expected)
  }
}
