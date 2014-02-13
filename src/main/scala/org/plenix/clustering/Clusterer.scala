package org.plenix.clustering

import org.plenix.similarity.SimilarityScorer
import scala.collection.GenMap

trait ClustererListener {
  def onPairs(pairs: Seq[(Int, Int)]) {}

  def onSingletons(singletons: Seq[Set[Int]]) {}

  def onPair(index: Int, pair: (Int, Int), clusterMap: GenMap[Int, Set[Int]]) {}

  def onClusters(clusters: Seq[Set[Int]]) {}
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
    val intScorers = scorers.map {
      case (scorer, minSimilarity) =>
        val intScorer = new SimilarityScorer[Int] {
          def score(index1: Int, index2: Int) = scorer.score(elements(index1), elements(index2))
        }
        (intScorer, minSimilarity)
    }

    // Perfom actual clustering
    val clusters = cluster(elements.size, pairs, intScorers: _*)

    // Map index clusters to their corresponding elements
    clusters.map {
      cluster => cluster.toSeq.sorted.map(elements(_))
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
  def cluster(size: Int, pairs: Seq[(Int, Int)], scorers: (SimilarityScorer[Int], Double)*): Seq[Set[Int]] = {
    val pairScores = buildScores(size, pairs, scorers: _*)
    cluster(size, pairScores)
  }

  def cluster(size: Int, pairScores: Seq[(Int, Int, Seq[Double])]): Seq[Set[Int]] = {
    /*
     * Order pairs in descending order of similarity (most similar pair first)
     * and remove scores as, after sorting, they're no longer needed
     */
    val sortedPairs: Seq[(Int, Int)] =
      pairScores
        .toVector
        .sortWith {
        (pairScore1, pairScore2) =>
          val (_, _, scores1) = pairScore1
          val (_, _, scores2) = pairScore2

          val nonEqualScores =
            scores1
              .zip(scores2)
              .dropWhile {
              case (score1, score2) => score1 == score2
            }

          nonEqualScores.length > 0 && {
            val (score1, score2) = nonEqualScores.head
            score2 < score1
          }
      }
        .par
        .map {
        case (index1, index2, _) => (index1, index2)
      }
        .seq

    doCluster(size, sortedPairs)
  }

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)]): Seq[Set[Int]] = {
    /*
 	 * Swap element indices in pairs to ensure all clusterable elements appear
     * in the connection map (built below)
     */
    val symmetricSortedPairs: Seq[(Int, Int)] = sortedPairs.par.map { case (index1, index2) => (index2, index1) }.seq

    /*
     * Build a connection map mapping each clusterable element index to the set
     * of its connected elements (where *connected* means similar above minimum
     * similarities for more than half the metrics)
     */
    val connectionMap: Map[Int, Set[Int]] =
      (sortedPairs ++ symmetricSortedPairs)
        .groupBy(_._1)
        .mapValues { values =>
          values.map { case (_, index2) => index2 }.toSet
        }

    doCluster(size, sortedPairs, connectionMap)
  }

  def doCluster(size: Int, sortedPairs: Seq[(Int, Int)], connectionMap: Map[Int, Set[Int]]) = {
    listener foreach (_.onPairs(sortedPairs))

    /*
     * Build the set of singleton non-clusterable clusters corresponding to elements
     * that didn't get connected to any other element because of low similarity scores
     */
    val singletonSets: Seq[Set[Int]] =
      (0 until size).par
        .filterNot(connectionMap.keySet.contains(_))
        .map(Set(_)).seq
    listener foreach (_.onSingletons(singletonSets))

    /*
     * Perform actual clustering by visiting each pair in similarity order
     * deciding how to cluster each element.
     */
    val clusterMap = collection.mutable.HashMap[Int, Set[Int]]()

    sortedPairs.foldLeft(1) { (count, pair) =>
      val (index1, index2) = pair

      listener foreach (_.onPair(count, pair, clusterMap))

      // Analyze whether each element in the pair is already clustered or not
      (clusterMap.contains(index1), clusterMap.contains(index2)) match {
        // 1) Neither is clustered: create a new cluster containing the two elements
        case (false, false) =>
          val cluster = Set(index1, index2)
          clusterMap(index1) = cluster
          clusterMap(index2) = cluster
        // 2) Both are clustered: if possible, merge the two clusters
        case (true, true) =>
          val cluster1 = clusterMap(index1)
          val cluster2 = clusterMap(index2)

          // Merge only if both cluster elements are fully interconnected
          val canMerge =
            cluster1.forall {
              element1 =>
                cluster2.forall {
                  element2 =>
                    connectionMap(element1).contains(element2)
                }
            }

          if (canMerge) {
            val mergedCluster = cluster1 ++ cluster2
            mergedCluster foreach {
              index => clusterMap(index) = mergedCluster
            }
          }
        // 3) One is clustered and the other is not: if possible, add unclustered to cluster
        case (first, _) =>
          val (clustered, unclustered) = if (first) (index1, index2) else (index2, index1)

          // Add to cluster only if connected to every cluster element
          if (clusterMap(clustered).forall(connectionMap(_).contains(unclustered))) {
            val cluster = clusterMap(clustered)

            val newCluster = cluster + unclustered
            newCluster foreach {
              index => clusterMap(index) = newCluster
            }
          } else {
            // Create a separate cluster with unclustered element
            clusterMap(unclustered) = Set(unclustered)
          }
      }
      count + 1
    }

    val clusterSets: Seq[Set[Int]] = clusterMap.map(_._2).toVector.distinct

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
      .map {
      case (index1, index2) =>
        val scores = scorers.map {
          case (scorer, _) => scorer.score(index1, index2)
        }
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
