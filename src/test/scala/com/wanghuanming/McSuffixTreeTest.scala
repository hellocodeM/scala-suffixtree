package com.wanghuanming


import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
  * Created by ming on 16-11-13.
  */
@RunWith(classOf[JUnitRunner])
class McSuffixTreeTest extends FunSuite {

  test("insertSuffix") {
    val tree = new McSuffixTree
    tree.insertSuffix(RangeSubString("hello", "1"))

    assert(tree.suffixesTest === Array("1:hello"))
  }

  test("branch") {
    val tree = new McSuffixTree
    tree.insertSuffix(RangeSubString("LLO$", "1"))
    tree.insertSuffix(RangeSubString("LO$", "2"))

    assert(tree.suffixesTest === Array("1:LLO$", "2:LO$"))
  }

  test("same suffix") {
    val tree = new McSuffixTree
    tree.insertSuffix(RangeSubString("LL0$", "1"))
    tree.insertSuffix(RangeSubString("LL0$", "2"))

    assert(tree.suffixesTest === Array("1:LL0$", "2:LL0$"))
  }

  def testInsertForStrings(strings: scala.collection.IndexedSeq[String]): Unit = {
    val tree = new McSuffixTree

    for (i <- strings.indices) {
      tree.insert(strings(i), i.toString)
    }

    assert(tree.suffixesTest === Utils.suffixes(strings: _*))
  }

  test("single string") {
    testInsertForStrings(Array("5Bfs5reMha"))
  }

  test("suffix strings") {
    testInsertForStrings(Array("1024", "24"))
  }

  test("substring strings") {
    testInsertForStrings(Array("1024", "02"))
  }

  test("prefix strings") {
    testInsertForStrings(Array("100", "10"))
  }

  test("prefix strings reverse") {
    testInsertForStrings(Array("10", "100"))
  }

  test("repfix strings repeated") {
    testInsertForStrings(Array("10", "100", "10", "100"))
  }

  test("two same string") {
    testInsertForStrings(Array("1024", "1024"))
  }

  test("head same with tail") {
    testInsertForStrings(Array("102410"))
  }

  test("plalindrome strings") {
    testInsertForStrings(Array("hellolleh"))
  }

  test("prefix repeated strings") {
    testInsertForStrings(Array("1", "11", "111", "11111"))
  }

  test("segment repeated strings") {
    testInsertForStrings(Array("Quu", "Qu"))
  }

  test("some randomly short string") {
    val n = 10
    val size = 10
    val tree = new McSuffixTree
    val sources = new mutable.ArrayBuffer[String]
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      sources += str
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    assert(tree.suffixesTest === strings.sorted, sources)
  }

  test("specific case") {
    testInsertForStrings(Array("rM6pXM9Osb", "QZW8xI04sU", "rM6pXM9Osb", "QZW8xI04sU"))
  }

  test("insert many randomly short string") {
    val n = 100
    val size = 10
    val tree = new McSuffixTree
    val sources = new mutable.ArrayBuffer[String]
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      sources += str
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    val res = tree.suffixesTest
    val expected = strings.sorted
    val diff = (res diff expected) ++ (expected diff res)

    assert(diff.isEmpty, debugDiff(diff, sources))
  }

  def debugDiff(diff: Iterable[String], source: collection.IndexedSeq[String]): Unit = {
    diff.foreach { item =>
      val index = item.takeWhile(_ != ':').toInt
      println(source(index))
    }
  }

  test("insert some randomly large string") {
    val n = 100
    val size = 1024
    val tree = new McSuffixTree
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    assert(tree.suffixesTest === strings.sorted)
  }

  test("buildByPrefix") {
    val str = "hello"
    val trees = McSuffixTree.buildByPrefix(str, "1")

    assert(trees.flatMap(_.suffixesTest).sorted === Utils.suffixesWithLabel("1", str))
  }
}

@RunWith(classOf[JUnitRunner])
class ExsetMcSuffixTreeTest extends FunSuite {

  def normalize(input: BufferedSource): String = {
    input.getLines().mkString
  }

  def expectedResult(name: String): Iterator[String] = {
    Source.fromFile(filePath(name)).getLines
  }

  def filePath(name: String): String = {
    val resourceDir = "src/test/resources/exset/"
    resourceDir + name
  }

  test("ex0") {
    val input = Array("ex0/1", "ex0/2")
    val tree = new McSuffixTree

    for (file <- input) {
      val str = normalize(Source.fromFile(filePath(file)))
      tree.insert(str, file)
    }

    tree.suffixes.foreach(println)

    expectedResult("res0").foreach(println)
  }
}

@RunWith(classOf[JUnitRunner])
class SparkMcSuffixTreeTest extends FunSuite with BeforeAndAfter {

  val conf = new SparkConf().setMaster("local[4]").setAppName("McSuffixTreeTest")
  var sc: SparkContext = _

  before {
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

  test("trivial") {
    val str = "hello"
    val strs = Array(RangeSubString(str, "txt1"))
    val alphabet = Utils.getAlphabet(strs)
    val terminal = "$"
    val prefixes = alphabet.map(_.toString)

    val trees = McSuffixTree.buildOnSpark(sc, strs, terminal, alphabet, prefixes)
    val suffixes = trees.flatMap(_.suffixesTest).collect().sorted
    assert(suffixes === Utils.suffixesWithLabel("txt1", str))
  }
}

