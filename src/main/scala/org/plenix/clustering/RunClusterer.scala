package org.plenix.clustering

import scala.io.Source
import org.plenix.similarity.SimilarityScorer
import org.plenix.similarity.string.LuceneLevenstein
import org.plenix.util.NGramUtils.ngramPairs
import com.typesafe.scalalogging.slf4j.Logging
import org.plenix.similarity.string.LuceneJaroWinkler
import org.apache.lucene.search.spell.JaroWinklerDistance

object RunClusterer extends App with Logging {
  def filename(part: String) = s"data/all_term${part}.dat"

//  val terms =
//    Source
//      .fromFile(filename("s"))
//      .getLines
//      .map(_.split('\t')(0)).toSeq

  val terms = Seq(
    "alejandro", "alejnadro", "alejo",
    "marlene", "marleny", "marleney",
    "marta", "martha",
    "ricardo")

  val stringScorers = Seq((LuceneJaroWinkler, 0.84), (LuceneLevenstein, 0.0))

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
  //  logger.info(s"Saving ${pairs.length} pairs")
  //  val pairsFile = outputFile("_pairs")
  //  pairs.foreach {
  //    case (index1, index2) => pairsFile.println(s"${terms(index1)}\t${terms(index2)}")
  //  }

  val clusterer = new Clusterer

  logger.info(s"Comparing ${pairs.length} pairs")
  val pairScores = clusterer.buildScores(terms.length, pairs, scorers: _*)
  //logger.info(s"Saving ${pairScores.length} scores")
  //val scoresFile = outputFile("_scores")
  //pairScores.foreach {
  //  case (index1, index2, scores) => scoresFile.println(s"${terms(index1)}\t${terms(index2)}\t${scores.mkString("\t")}")
  //}

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