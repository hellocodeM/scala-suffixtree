package com.wanghuanming

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ming on 16-11-12.
  */
trait SubString {
  val source: String
  val start: Int
  val end: Int
  val label: String
}

case class RangeSubString(source: String, start: Int, end: Int, label: String) extends SubString {

  def apply(idx: Int): Char = source(start + idx)

  def length = end - start

  def isEmpty = source.isEmpty || start >= end

  def nonEmpty = source.nonEmpty && start < end

  def substring(s: Int) = RangeSubString(source, start + s, end, label)

  def substring(s: Int, e: Int) = RangeSubString(source, start + s, start + e, label)

  def head: Char = source(start)

  def commonPrefix(rhs: RangeSubString): RangeSubString = {
    val len = length min rhs.length
    for (i <- 0 until len) {
      if (this (i) != rhs(i)) {
        return substring(0, i)
      }
    }
    this
  }

  def take(n: Int) = substring(0, n)

  def drop(n: Int) = substring(n, length)

  def mkString = source.substring(start, end)

  override def toString = mkString
}

class TreeNode {

  lazy val children = new mutable.HashMap[Char, TreeEdge]

  def childEdge(ch: Char): TreeEdge = children(ch)

  def childNode(ch: Char): TreeNode = children(ch).targetNode

  def addChild(str: RangeSubString): Unit = {
    children += str.head -> TreeEdge(str, new TreeNode)
  }

  def addEdge(edge: TreeEdge): Unit = {
    children += edge.seq.head -> edge
  }
}

case class TreeEdge(var seq: RangeSubString, targetNode: TreeNode) {

  def splitAt(idx: Int): TreeEdge = {
    val x = TreeEdge(seq.take(idx), new TreeNode())
    val y = TreeEdge(seq.drop(idx), targetNode)
    x.targetNode.addEdge(y)
    x
  }
}

class SuffixTree {
  val root = new TreeNode()

  def insert(str: String, label: String): Unit = {
    val mangled = str + '$'

    for (s <- 0 to mangled.length) {
      insertSuffix(RangeSubString(mangled, s, mangled.length, label))
    }
  }

  def insertSuffix(origin: RangeSubString): Unit = {
    var iter = root
    var sub = origin

    while (sub.nonEmpty) {
      val ch = sub.head

      if (!iter.children.isDefinedAt(ch)) {
        iter.addChild(sub)
        return
      } else {
        val edge = iter.children(ch)
        val cp = edge.seq.commonPrefix(sub)

        if (edge.seq.length - cp.length > 0) {
          // split origin edge
          iter.children.update(edge.seq.head, edge.splitAt(cp.length))
        }
        sub = sub.drop(cp.length)
        iter = iter.childNode(ch)
      }
    }
  }


  def strings: Array[String] = {
    val res = new ArrayBuffer[String]
    val buff = new StringBuffer()

    def dfs(r: TreeNode): Unit = {
      if (r.children.isEmpty) {
        res += buff.toString
      } else {
        for ((ch, edge) <- r.children) {
          buff.append(edge.seq)
          dfs(edge.targetNode)
          buff.setLength(buff.length - edge.seq.length)
        }
      }
    }
    dfs(root)
    res.toArray
  }

  def nodes: Iterator[TreeNode] = {
    new PrefixNodeIterator
  }

  def edges: Iterator[TreeEdge] = {
    throw new UnsupportedOperationException
  }

  class PrefixNodeIterator extends Iterator[TreeNode] {

    private val toVisit = mutable.Stack[TreeNode](root)

    override def hasNext: Boolean = {
      toVisit.nonEmpty
    }

    override def next(): TreeNode = {
      val res = toVisit.pop
      toVisit.pushAll(res.children.values.map(_.targetNode))
      res
    }
  }
}
