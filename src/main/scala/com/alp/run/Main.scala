/**
 * author: Kim Junho
 * email: lunaticfoxy@gmail.com
 */
package com.alp.run

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.{Row, SparkSession}
import com.alp.struct.{NetworkEmbedding, Params}
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StructField, StructType}
import scopt.OptionParser

/**
 * Network Embedding을 위한 기본 프로세스를 담은 싱글톤 객체
 */
object Main {

  /**
   * 실제 동작을 수행하는 함수
   * @param spark 스파크 세션
   * @param params 파라미터
   * @param saveTo Hive 테이블에 결과 저장시 활용
   */
  def run(spark: SparkSession, params: Params = Params(), saveTo: String = null) = {
    val sc: SparkContext = spark.sparkContext

    // 사용할 파라미터 정리
    val numVerts = params.numVerts
    val numEdges = params.numEdges
    val numPartitions = params.numPartitions
    val vectorSize = params.vectorSize
    val walkLength = params.walkLength
    val numWalks = params.numWalks
    val p = params.p
    val q = params.q

    // 실험을 위한 랜덤 그래프 생성
    val graph: Graph[Long, Long] = GraphGenerators.rmatGraph(sc, numVerts, numEdges).mapVertices((v, _) => v.toLong).mapEdges(e => e.attr.toLong)

    // 그래프를 재귀적으로 랜덤 파티셔닝
    val partitionedInfo = NetworkEmbedding.randomPartitioning(spark, graph, numVerts, numPartitions, vectorSize)

    // 파티션된 정보를 순회하며 Embedding 벡터를 계산하고 재귀적으로 통합
    val embeddingVector = NetworkEmbedding.mergePartitionedVector(spark, partitionedInfo, walkLength, numWalks, p, q)

    // 30개 데이터만 stdout에 출력
    println("======= Embedding Vector (Sample 30 Nodes) =======")
    embeddingVector.take(Math.min(numVerts, 30)).map(x => s"${x._1} : ${x._2.mkString(",")}").mkString("\n")

    if (saveTo != null) { // 저장 위치 입력시 테이블에 저장
      val scheme = StructType(Seq(StructField("vertex", LongType), StructField("vector", ArrayType(DoubleType))))
      val df = spark.createDataFrame(embeddingVector.map(x => Row(x._1, x._2)), scheme)
      df.write.mode("overwrite").saveAsTable(saveTo)
    }
  }

  /**
   * 커맨드라인으로붜 파라미터를 받아오는 메인함수
   * @param args
   */
  def main(args: Array[String]) = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("NetworkEmbedding") {
      head("Main")
      opt[Int]("numVerts")
        .text(s"numVerts: ${defaultParams.numVerts}")
        .action((x, c) => c.copy(numWalks = x))
      opt[Int]("numEdges")
        .text(s"numEdges: ${defaultParams.numEdges}")
        .action((x, c) => c.copy(numEdges = x))
      opt[Int]("numPartitions")
        .text(s"numPartitions: ${defaultParams.numPartitions}")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Int]("vectorSize")
        .text(s"numPartitions: ${defaultParams.vectorSize}")
        .action((x, c) => c.copy(vectorSize = x))
      opt[Int]("walkLength")
        .text(s"walkLength: ${defaultParams.walkLength}")
        .action((x, c) => c.copy(walkLength = x))
      opt[Int]("numWalks")
        .text(s"numWalks: ${defaultParams.numWalks}")
        .action((x, c) => c.copy(numWalks = x))
      opt[Double]("p")
        .text(s"return parameter p: ${defaultParams.p}")
        .action((x, c) => c.copy(p = x))
      opt[Double]("q")
        .text(s"in-out parameter q: ${defaultParams.q}")
        .action((x, c) => c.copy(q = x))
    }

    val params = parser.parse(args, defaultParams).head

    // 스파크 세션 생성
    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.instances", "32")
      .config("spark.executor.cores", "4")
      .config("spark.shuffle.useOldFetchProtocol", "true")
      .config("dynamicAllocation.enabled", "true")
      .config("dynamicAllocation.maxExecutors", "300")
      .getOrCreate()

    // 동작 시작
    run(spark, params)
  }
}
