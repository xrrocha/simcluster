package org.plenix.clustering

import org.scalatest.FunSuite
import org.plenix.similarity.string.LuceneLevenstein
import org.plenix.similarity.string.LuceneJaroWinkler
import scala.io.Source

class ClustererTest extends FunSuite {
  import ClustererTest._

  test("Clusters test surnames correctly") {
    val names = readResource("surnames.txt")
    val scorers = Seq((LuceneJaroWinkler, .875), (LuceneLevenstein, .78))

    val clusterer = new Clusterer
    val actualClusters = clusterer.cluster(names, scorers: _*).sortBy(-_.size)
    val expectedClusters = readResource("surname-clusters.txt").map { _.split(',').toSeq }

    assert(actualClusters == expectedClusters)
  }
}

object ClustererTest {
  def readResource(resourceName: String) = {
    val is = getClass.getClassLoader.getResourceAsStream(resourceName)
    Source.fromInputStream(is).getLines.toVector
  }
}