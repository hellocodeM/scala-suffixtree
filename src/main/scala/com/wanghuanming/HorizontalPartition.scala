package com.wanghuanming

/**
  * Created by ming on 16-11-15.
  */
object HorizontalPartition {

  type SPrefix = String

  /*
  def partition(s: String, sprefix: SPrefix): SuffixTree = {
    val tree = new SuffixTree
    val u = new TreeNode
    val e = TreeEdge(RangeSubString(sprefix, 0, sprefix.length, "shit"), u)
    tree.root.addEdge(e)

    branchEdge(s, tree, e)
    tree
  }

  def computeY(s: String, seq: RangeSubString) = ???

  def pathLabel(tree: SuffixTree, e: TreeEdge) = ???

  def branchEdge(s: String, tree: SuffixTree, e: TreeEdge): Unit ={
    val y: Iterable[Char] = computeY(s, e.seq) // a set containingthe symbols that follow pathlabel(s) in S
    val pl: String = pathLabel(tree, e)

    if (s.grouped(pl.length).count(_ == pl) == 1) {
      // leaf node
      e.seq += '$'
    } else if (y.size == 1) {
      // same symbol s1 after pathlabel(e) in S
      val symbol: Char = y.head
      e.seq += symbol
      branchEdge(s, tree, e)
    } else {
      for (symbol <- y) {
        val e1 = new TreeEdge(symbol, new TreeNode)
        e.targetNode.addEdge(e1)
        branchEdge(s, tree, e1)
      }
    }
  }
  */

}
