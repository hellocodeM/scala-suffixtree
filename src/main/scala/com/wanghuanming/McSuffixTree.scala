package com.wanghuanming

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// todo: identifier for same suffix in different input string
trait TreeNode {

  var seq: RangeSubString

  def updateChild(ch: Char, edge: TreeNode): Unit

  def children: scala.collection.Map[Char, TreeNode]

  def hasChild(ch: Char) = false

  def childNode(ch: Char): TreeNode

  def addChild(str: RangeSubString): Unit
}

class LeafNode(override var seq: RangeSubString) extends TreeNode {

  val terminals = new ArrayBuffer[RangeSubString]

  override def childNode(ch: Char): TreeNode = {
    throw new IllegalStateException("leaf node has no child node")
  }

  override def addChild(str: RangeSubString): Unit = {
    assert(str.head == '$')
    terminals += str
  }

  override def children: Map[Char, TreeNode] = {
    throw new IllegalStateException()
  }

  override def updateChild(ch: Char, edge: TreeNode): Unit = {
    throw new IllegalStateException()
  }

  override def toString: String = {
    s"LeafNode($seq)"
  }
}

class BranchNode(override var seq: RangeSubString) extends TreeNode {

  override lazy val children = new mutable.HashMap[Char, TreeNode]

  override def hasChild(ch: Char) = children.contains(ch)

  override def childNode(ch: Char): TreeNode = children(ch)

  override def addChild(str: RangeSubString): Unit = {
    children += str.head -> new LeafNode(str)
  }

  def addChild(node: TreeNode): Unit = {
    children += node.seq.head -> node
  }

  override def updateChild(ch: Char, node: TreeNode): Unit = {
    children.update(ch, node)
  }

  override def toString: String = {
    s"BranchNode($seq)"
  }
}

/*
// todo: migrate seq to targetNode
case class TreeEdge(var seq: RangeSubString, targetNode: TreeNode) {

  def splitAt(length: Int): TreeEdge = {
    val front = TreeEdge(seq.take(length), new BranchNode())
    val back = TreeEdge(seq.drop(length), targetNode)
    front.targetNode.addEdge(back)
    front
  }
}
*/

class McSuffixTree {

  val root: TreeNode = new BranchNode(null)
  private var stringCnt = -1

  private def terminalSymbol: String = {
    stringCnt = stringCnt + 1
    "$" + stringCnt
  }

  def insert(str: String, label: String): Unit = {
    // insert all suffixes
    // todo: insert different terminal symbol for different string
    val S = str + terminalSymbol
    for (s <- 0 to str.length) {
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
      } else if (ch == '$') {
        // same string, but with different terminal symbol
        val leaf = iter.childNode(ch)
        // todo: use another word instead of addChild
        leaf.addChild(remain)
        remain = remain.take(0)
      } else {
        val child = iter.children(ch)
        val cp = child.seq.commonPrefix(remain)

        if (cp.isEmpty) {
          iter.addChild(remain)
          remain = remain.take(0)
        } else if (child.seq.length > cp.length) {
          // split origin child
          iter.updateChild(ch, splitEdgeAt(child, cp.length))
          remain = remain.drop(cp.length)
        }
      }
      iter = iter.childNode(ch)
    }
  }


  /**
    * @return originally inserted suffixes.
    */
  def suffixes: Set[String] = {
    val res = new mutable.HashSet[String]()
    val buff = new ArrayBuffer[String]()

    def dfs(r: TreeNode): Unit = {
      r match {
        case x: LeafNode =>
          // one leaf node contains multi string
          val prefix = buff.init.mkString
          (x.terminals.map(_.mkString) :+ buff.last).foreach(postfix =>
            res += prefix + postfix
          )
        case _: BranchNode =>
          for ((ch, child) <- r.children) {
            buff += child.seq.mkString
            dfs(child)
            buff.reduceToSize(buff.length - 1)
          }
      }
    }
    dfs(root)
    res.toSet
  }

  def debugDump(): Unit = {
    val buff = new mutable.ArrayBuffer[String]

    def dfs(r: TreeNode): Unit = {
      r match {
        case x: LeafNode =>
          val prefix = buff.init.mkString
          (x.terminals.map(_.mkString) :+ buff.last).foreach(postfix =>
            println(prefix + postfix)
          )
        case x: BranchNode =>
          for ((ch, child) <- r.children) {
            buff += child.seq.mkString
            dfs(child)
            buff.reduceToSize(buff.length - 1)
          }
      }
    }

    dfs(root)
  }
}
