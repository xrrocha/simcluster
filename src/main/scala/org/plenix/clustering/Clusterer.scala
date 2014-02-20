package org.plenix.clustering

import org.plenix.similarity.SimilarityScorer

trait ClustererListener {
  def onPairs(pairs: Seq[(Int, Int)]) {}

  //def onSingletons(singletons: Seq[Seq[Int]]) {}

  def onPair(count: Int, index1: Int, index2: Int) {}

  def onClusters(clusters: Seq[Seq[Int]]) {}
}

trait Cluster[T] {
  def toSeq: Seq[T] = root.doToSeq

  def forall(predicate: T => Boolean): Boolean = root.doForall(predicate)

  // TODO Hide do* methods
  def doToSeq: Seq[T]

  def doForall(predicate: T => Boolean): Boolean

  var parent: Option[Cluster[T]] = None

  def root: Cluster[T] = if (parent.isDefined) parent.get.root else this
}

case class SingleCluster[T](t: T) extends Cluster[T] {
  def doToSeq = Seq(t)

  def doForall(predicate: T => Boolean) = predicate(t)
}

case class PairCluster[T](left: T, right: T) extends Cluster[T] {
  def doToSeq = Seq(left, right)

  def doForall(predicate: T => Boolean) = predicate(left) && predicate(right)
}

case class ParentCluster[T](left: Cluster[T], right: Cluster[T]) extends Cluster[T] {
  left.root.parent = Some(this)
  right.root.parent = Some(this)

  def doToSeq = left.doToSeq ++ right.doToSeq

  def doForall(predicate: T => Boolean) = left.doForall(predicate) && right.doForall(predicate)
}

/**
 * A simple agglomerative clusterer supporting multiple concurrent similarity metrics.
 */
class Clusterer(listener: Option[ClustererListener] = None) {
  /**
   * Cluster a sequence of elements based on one or more similarity metrics, each
   * a with minimum threshold.
   * This method builds a default pair set formed by the (reduced) cartesian product
   * of the input sequence of elements
   */
  def cluster[T](elements: Seq[T], scorers: (SimilarityScorer[T], Double)*): Seq[Seq[T]] = {
    // Default pair generator: non-redundant cartesian product
    val pairs = for {
      i <- 0 until elements.size
      j <- i + 1 until elements.size
    } yield (i, j)

    // Map user-provided similarity scorers to operate on indexes
    val intScorers = scorers.map { case (scorer, minSimilarity) =>
      val intScorer = new SimilarityScorer[Int] {
        def score(index1: Int, index2: Int) = scorer.score(elements(index1), elements(index2))
      }
      (intScorer, minSimilarity)
    }

    // Perfom actual clustering
    val clusters = cluster(elements.size, pairs, intScorers: _*)

    // Map index clusters to their corresponding elements
    clusters.map { cluster => cluster.toSeq.sorted.map(elements(_))
    }
  }

  /**
   * Cluster a sequence of index pairs based on one or more similarity metrics, each
   * a with minimum threshold.
   *
   * Elements are represented as integer indices. Elements must be provided in pairs so as to
   * decouple clustering from pair generation (which can be cartesian product or some blocking
   * algorithm).
   *
   * Each similarity scorer has an associated minimum similarity below which a pair cannot be
   * considered clusterable. Scorer order is important with the most influential metric appearing
   * first. At least half of the metrics have to yield scores above their minimums for the pair
   * to be included in the clustering process.
   */
  def cluster(size: Int, pairs: Seq[(Int, Int)], scorers: (SimilarityScorer[Int], Double)*): Seq[Seq[Int]] = {
    val pairScores = buildScores(size, pairs, scorers: _*)
    cluster(size, pairScores)
  }

  def cluster(size: Int, pairScores: Seq[(Int, Int, Seq[Double])]): Seq[Seq[Int]] = {
    /*
     * Order pairs in descending order of similarity (most similar pair first)
     * and remove scores as, after sorting, they're no longer needed
     */
    val sortedPairs: Seq[(Int, Int)] =
      pairScores
        .toVector
        .sortWith { (pairScore1, pairScore2) =>
        val (_, _, scores1) = pairScore1
        val (_, _, scores2) = pairScore2

        val nonEqualScores =
          scores1
            .zip(scores2)
            .dropWhile { case (score1, score2) => score1 == score2}

        nonEqualScores.length > 0 && {
          val (score1, score2) = nonEqualScores.head
          score2 < score1
        }
      }
        .par
        .map { case (index1, index2, _) => (index1, index2)}
        .seq

    doCluster(size, sortedPairs)
  }

  import collection.mutable.{Set => MSet}
  case class Element(index: Int) {
    val connections: MSet[Int] = MSet()
    val cluster: Cluster[Int] = SingleCluster(index)

    def root = cluster.root

    def connectedTo(index: Int) = connections.contains(index)
    def addConnection(connectionIndex: Int) { connections += connectionIndex }
  }

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)]): Seq[Seq[Int]] = {
    /*
     * Build a connection map mapping each clusterable element index to the set
     * of its connected elements (where *connected* means similar above minimum
     * similarities for more than half the metrics)
     */
    val clusterElements = new Array[Element](size)
    (0 until size).par foreach(index => clusterElements(index) = Element(index))
    sortedPairs.par foreach{ case(index1, index2) =>
      clusterElements(index1).addConnection(index2)
      clusterElements(index2).addConnection(index1)
    }

    doCluster(size, sortedPairs, clusterElements)
  }

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)], clusterElements: Array[Element]) = {
    listener foreach (_.onPairs(sortedPairs))

    /*
     * Perform actual clustering by visiting each pair in similarity order
     * deciding how to cluster each element.
     */
    sortedPairs.foldLeft(0) { (count, pair) =>
      val (index1, index2) = pair
      listener foreach (_.onPair(count, index1, index2))

      val cluster1 = clusterElements(index1).root
      val cluster2 = clusterElements(index2).root

      // Merge only if both cluster elements are fully interconnected
      val canMerge =
        cluster1.forall { element1 =>
          cluster2.forall { element2 =>
            clusterElements(element1).connectedTo(element2)
          }
        }

      if (canMerge) {
        ParentCluster(cluster1, cluster2)
      }

      count + 1
    }

    val clusters: Seq[Seq[Int]] = clusterElements.map(_.root).toSeq.distinct.map(_.toSeq)
    listener.foreach(_.onClusters(clusters))

    clusters
  }

  /*
     * Consume input pairs computing similarity scores and filtering out those pairs
     * with less than half of successful scores.
     */
  def buildScores(size: Int, pairs: Seq[(Int, Int)], scorers: (SimilarityScorer[Int], Double)*): Seq[(Int, Int, Seq[Double])] =
    pairs
      .par
      .map { case (index1, index2) =>
      val scores = scorers.map { case (scorer, _) => scorer.score(index1, index2)}
      (index1, index2, scores)
    }
      .filter {
      case (_, _, scores) =>
        val matchCount = scores
          .zip(scorers.map(_._2))
          .takeWhile { case (sim, minSim) => sim >= minSim}
          .length
        matchCount >= (scorers.length / 2.0).round
    }
      .seq
}
