package phu.quang.le.batch

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

case class Page(id: Long, rank: Double)
case class Adjacency(id: Long, neighbors: Array[Long])

object PageRankTest extends App {
  private final val DAMPENING_FACTOR: Double = 0.85
  private final val NUM_VERTICES = 82140L;
  private final val RANDOM_JUMP = (1 - DAMPENING_FACTOR) / NUM_VERTICES;

  val env = ExecutionEnvironment.getExecutionEnvironment

  val rawLines: DataSet[String] = env.fromElements(
    "1 2 3 4",
    "2 1",
    "3 5",
    "4 2 3",
    "5 2 4"
  )

  val adjacency: DataSet[Adjacency] = rawLines
    .map(str => {
      val elements = str.split(' ')
      val id = elements(0).toLong
      val neighbors = elements.slice(1, elements.length).map { _.toLong }
      Adjacency(id, neighbors)
    })

  val initialRanks: DataSet[Page] = adjacency.map { adj => Page(adj.id, 1.0 / NUM_VERTICES) }
  
  val rankContributions = initialRanks.join(adjacency).where("id").equalTo("id") {
    (page, adj, out: Collector[Page]) => {
      val rankPerTarget = DAMPENING_FACTOR * page.rank / adj.neighbors.length
      out.collect(Page(page.id, RANDOM_JUMP))
      for (neighbor <- adj.neighbors) {
        out.collect(Page(neighbor, rankPerTarget))
      }
    }
  }
  rankContributions.print()
  val newRanks = rankContributions.groupBy("id").reduce((a, b) => Page(a.id, a.rank + b.rank))
  newRanks.print()
  
}