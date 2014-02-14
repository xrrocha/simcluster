package org.plenix.clustering

import org.plenix.similarity.SimilarityScorer

trait ClustererListener {
  def onPairs(pairs: Seq[(Int, Int)]) {}

  def onSingletons(singletons: Seq[Seq[Int]]) {}

  def onPair(pair: (Int, Int), clusterMap: Map[Int, Cluster[Int]]) {}

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

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)]): Seq[Seq[Int]] = {
    /*
 	 * Swap element indices in pairs to ensure all clusterable elements appear
     * in the connection map (built below)
     */
    val symmetricSortedPairs: Seq[(Int, Int)] = sortedPairs.par.map { case (index1, index2) => (index2, index1)}.seq

    /*
     * Build a connection map mapping each clusterable element index to the set
     * of its connected elements (where *connected* means similar above minimum
     * similarities for more than half the metrics)
     */
    val connectionMap: Map[Int, Seq[Int]] =
      (sortedPairs ++ symmetricSortedPairs)
        .groupBy(_._1)
        .mapValues { values => values.map { case (_, index2) => index2}}

    doCluster(size, sortedPairs, connectionMap)
  }

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)], connectionMap: Map[Int, Seq[Int]]) = {
    listener foreach (_.onPairs(sortedPairs))

    /*
     * Build the set of singleton non-clusterable clusters corresponding to elements
     * that didn't get connected to any other element because of low similarity scores
     */
    val singletonSets: Seq[Seq[Int]] =
      (0 until size).par
        .filterNot(connectionMap.keySet.contains)
        .map(Seq(_)).seq
    listener foreach (_.onSingletons(singletonSets))

    /*
     * Perform actual clustering by visiting each pair in similarity order
     * deciding how to cluster each element.
     */
    // TODO: Use mutable array instead of immutable map?
    val clusterMap = sortedPairs.foldLeft(Map[Int, Cluster[Int]]()) { (clusterMap, pair) =>
      val (index1, index2) = pair

      listener foreach (_.onPair(pair, clusterMap))

      // Analyze whether each element in the pair is already clustered or not
      (clusterMap.contains(index1), clusterMap.contains(index2)) match {
        // 1) Neither is clustered: create a new cluster containing the two elements
        case (false, false) =>
          val cluster = PairCluster(index1, index2)
          clusterMap + (index1 -> cluster) + (index2 -> cluster)

        // 2) Both are clustered: if possible, merge the two clusters
        case (true, true) =>
          val cluster1 = clusterMap(index1).root
          val cluster2 = clusterMap(index2).root

          // Merge only if both cluster elements are fully interconnected
          val canMerge =
            cluster1.forall { element1 =>
              cluster2.forall { element2 =>
                connectionMap(element1).contains(element2)
              }
            }

          if (canMerge) {
            ParentCluster(cluster1, cluster2)
          }

          clusterMap

        // 3) One is clustered and the other is not: if possible, add unclustered to cluster
        case (first, _) =>
          val (clustered, unclustered) = if (first) (index1, index2) else (index2, index1)

          val singleCluster = SingleCluster(unclustered)

          // Add to cluster only if connected to every cluster element
          val cluster = clusterMap(clustered).root
          if (cluster.forall(connectionMap(_).contains(unclustered))) {
            ParentCluster(cluster, singleCluster)
          }

          clusterMap + (unclustered -> singleCluster)
      }
    }

    val clusterSets: Seq[Seq[Int]] = clusterMap.values.map(_.root).toSeq.distinct.map(_.toSeq)

    // Return clusters and singletons together
    val clusters = clusterSets ++ singletonSets
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
