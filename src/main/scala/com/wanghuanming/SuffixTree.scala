package com.wanghuanming

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ming on 16-11-12.
  */
case class TreeNode(var value: Int = -1,
                    var source: String = null,
                    var children: Array[TreeNode] = null)

class SuffixTree {
  val numCharacters = 26

  var root = new TreeNode

  def insert(str: String, value: Int, source: String): Unit = {
    var iter = root
    for (ch <- str) {
      if (iter.children == null) {
        iter.children = new Array[TreeNode](numCharacters)
      }
      if (iter.children(indexOfChar(ch)) == null) {
        iter.children(indexOfChar(ch)) = new TreeNode
      }
      iter = iter.children(indexOfChar(ch))
    }
    iter.value = value
    iter.source = source
  }

  def insertAllSuffix(str: String, source: String): Unit = {
    for (i <- 0 until str.length) {
      insert(str.substring(i), i, source)
    }
  }

  def foreachLeaf(fn: (TreeNode, Int) => Unit): Unit = {
    def dfs(height: Int, root: TreeNode, fn: (TreeNode, Int) => Unit): Unit = {
      if (root != null ) {
        // leaf node
        if (root.children == null) {
          fn(root, height)
        } else {
          // branch node
          root.children.foreach(x => dfs(height+1, x, fn))
        }
      }
    }
    dfs(0, root, fn)
  }

  def foreachLeaf(fn: TreeNode => Unit): Unit = {
    foreachLeaf((node: TreeNode, y: Int) => fn(node))
  }

  def leaves: Array[TreeNode] = {
    val res = new ArrayBuffer[TreeNode]
    foreachLeaf(node => res += node)
    res.toArray
  }

  def leavesWithHeight: Array[(TreeNode, Int)] = {
    val res = new ArrayBuffer[(TreeNode, Int)]()
    foreachLeaf((node, h) => res += node -> h)
    res.toArray
  }

  private def indexOfChar(ch: Char): Int = {
    ch - 'a'
  }
}

object SuffixTree {

  case class StringWithTag(str: String, tag: String)

  def fromStrings(strs: Iterable[StringWithTag]): SuffixTree = {
    val tree = new SuffixTree
    strs.foreach(x => tree.insertAllSuffix(x.str, x.tag))
    tree
  }

  def fromFile(filename: String): SuffixTree = {
    throw new UnsupportedOperationException
  }
}
