package org.plenix.clustering

import io.Source
import org.plenix.similarity.string.{SecondStringSoftTFIDF, LuceneLevenstein, LuceneJaroWinkler}
import org.plenix.similarity.SimilarityScorer
import org.plenix.util.NGramUtils._
import com.typesafe.scalalogging.slf4j.Logging
import com.wcohen.ss.JaroWinkler

object ClusterNames extends Logging {
  def filename(part: String) = s"data/school-names.txt"

  val terms =
    Source
      .fromFile(filename("s"))
      .getLines()
      .map(_.split('\t')(0)).toSeq

  val stringScorers = Seq((new SecondStringSoftTFIDF(terms, new JaroWinkler, .9), .9))

  val scorers =
    stringScorers
      .map {
      case (scorer, minSimilarity) =>
        val intScorer = new SimilarityScorer[Int] {
          def score(index1: Int, index2: Int) = scorer.score(terms(index1), terms(index2))
        }
        (intScorer, minSimilarity)
    }
  logger.info(s"Prepared scorers: ${stringScorers.map(_._1.getClass.getSimpleName).mkString(",")}")

  logger.info(s"Finding pairs for ${terms.length} terms")
  val pairs = ngramPairs(terms, 3)

  val clusterer = new Clusterer

  logger.info(s"Comparing ${pairs.length} pairs")
  val pairScores = clusterer.buildScores(terms.length, pairs, scorers: _*)

  logger.info(s"Clustering ${pairs.length} pairs")
  val clusters = clusterer.cluster(terms.length, pairScores).sortBy(-_.size)
  logger.info(s"Clustered ${clusters.flatten.length}/${terms.length} into ${clusters.length} groups")

  logger.info(s"Saving clusters")
  val clustersFile = outputFile("_clusters")
  clusters
    .zipWithIndex
    .foreach {
    case (cluster, clusterIndex) =>
      cluster
        .toSeq
        .sorted
        .zipWithIndex
        .foreach {
        case (element, elementIndex) =>

          clustersFile.println(s"${clusterIndex + 1}\t${terms(element)}")
      }
  }
  logger.info("Done")

  import java.io.FileWriter
  import java.io.PrintWriter
  def outputFile(part: String) = new PrintWriter(new FileWriter(filename(part)), true)
}
