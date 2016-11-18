package com.wanghuanming

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait TreeNode {

  var seq: RangeSubString

  def updateChild(ch: Char, edge: TreeNode): Unit

  def children: scala.collection.Map[Char, TreeNode]

  def hasChild(ch: Char) = false

  def childNode(ch: Char): TreeNode

  def addChild(str: RangeSubString): Unit

  def addChild(node: TreeNode): Unit
}

class LeafNode(override var seq: RangeSubString) extends TreeNode {

  // additional terminal for same leaf node
  // why not include seq into terminals: seq is variable, it could be changed when split edge, but terminal does not.
  val terminals = new ArrayBuffer[RangeSubString]

  override def childNode(ch: Char): TreeNode = {
    throw new IllegalStateException("leaf node has no child node")
  }

  override def addChild(str: RangeSubString): Unit = {
    throw new IllegalStateException()
  }

  override def children: Map[Char, TreeNode] = {
    throw new IllegalStateException()
  }

  override def updateChild(ch: Char, edge: TreeNode): Unit = {
    throw new IllegalStateException()
  }

  override def addChild(node: TreeNode): Unit = {
    throw new IllegalStateException()
  }

  override def toString: String = {
    s"LeafNode($seq, ${terminals.mkString(",")})"
  }

  def addTerminal(terminal: RangeSubString): Unit = {
    terminals += terminal
  }
}

class BranchNode(override var seq: RangeSubString) extends TreeNode {

  override lazy val children = new mutable.HashMap[Char, TreeNode]

  override def hasChild(ch: Char) = children.contains(ch)

  override def childNode(ch: Char): TreeNode = children(ch)

  override def addChild(str: RangeSubString): Unit = {
    children += str.head -> new LeafNode(str)
  }

  override def addChild(node: TreeNode): Unit = {
    children += node.seq.head -> node
  }

  override def updateChild(ch: Char, node: TreeNode): Unit = {
    children.update(ch, node)
  }

  override def toString: String = {
    s"BranchNode($seq)"
  }
}

class McSuffixTree {

  val root: TreeNode = new BranchNode(null)

  val terminalSymbol = "$"

  def insert(str: String, label: String): Unit = {
    // insert all suffixes
    val S = str + terminalSymbol
    for (s <- S.indices) {
      insertSuffix(RangeSubString(S, s, S.length, label))
    }
  }

  /**
    * r =seq=> origin
    *
    * r =front=> middle =end=> origin
    */
  private def splitEdgeAt(origin: TreeNode, length: Int): TreeNode = {
    val front = origin.seq.take(length)
    val back = origin.seq.drop(length)
    val middle = new BranchNode(front)
    origin.seq = back
    middle.addChild(origin)
    middle
  }


  def insertSuffix(origin: RangeSubString): Unit = {
    var iter = root
    var remain = origin

    while (remain.nonEmpty) {
      val ch = remain.head

      if (!iter.hasChild(ch)) {
        iter.addChild(remain)
        remain = remain.drop(remain.length)
      } else {
        val child = iter.children(ch)
        val cp = child.seq.commonPrefix(remain)

        if (cp.length < child.seq.length) {
          // split origin child
          iter.updateChild(ch, splitEdgeAt(child, cp.length))
        } else if (cp.length == remain.length) {
          // remain is the leaf
          val leaf = child.asInstanceOf[LeafNode]
          leaf.addTerminal(remain)
        }
        remain = remain.drop(cp.length)
      }
      iter = iter.childNode(ch)
    }
  }


  /**
    * @return originally inserted suffixes.
    */
  def suffixes: Array[String] = {
    val res = new mutable.ArrayBuffer[String]()
    val buff = new ArrayBuffer[String]()

    def dfs(r: TreeNode): Unit = {
      r match {
        case x: LeafNode =>
          // one leaf node contains multi string
          val prefix = buff.init.mkString
          x.terminals.foreach(terminal =>
            res += terminal.label + ":" + prefix + terminal.mkString
          )
          res += x.seq.label + ":" + prefix + x.seq
        case _: BranchNode =>
          for ((ch, child) <- r.children) {
            buff += child.seq.mkString
            dfs(child)
            buff.reduceToSize(buff.length - 1)
          }
      }
    }
    dfs(root)
    res.toArray.sorted
  }
}

object McSuffixTree {

  def buildByPrefix(str: String, label: String): Array[McSuffixTree] = {
    val alphabet = str.distinct
    val S = str + '$'
    (alphabet :+ '$').par.map { prefix =>
      val tree = new McSuffixTree
      for (i <- S.indices) {
        if (S(i) == prefix) {
          tree.insertSuffix(RangeSubString(S, i, S.length, label))
        }
      }
      tree
    }.toArray
  }

}
