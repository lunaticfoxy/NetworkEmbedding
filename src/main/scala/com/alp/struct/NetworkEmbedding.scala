/**
 * author: Kim Junho
 * email: lunaticfoxy@gmail.com
 */
package com.alp.struct

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Network Embedding을 과정에서 발생하는 큰규모 함수를 담은 싱글톤 객체
 * - 전체 그래프 분할, 파티션간의 연산, 임베딩 벡터 통합 등 포함
 */
object NetworkEmbedding {
  /**
   * 재귀적으로 파티션을 랜덤으로 분할하는 함수
   * 파티션 개수만큼의 임의의 지점을 시작점으로 지정
   * 각 파티션이 시작지점에서부터 super step 단계마다 한 노드씩 영역을 넓혀가며 타 파티션과 만나는 지점까지 영역 확장
   *
   * @param spark         스파크 세션
   * @param graph         분할대상 그래프
   * @param numVerts      그래프의 vertex 수
   * @param numPartitions 분할할 파티션 수
   * @param vectorSize    임베딩 벡터 사이즈
   * @param res           리턴할 결과 (꼬리재귀를 위해 입력 파라미터로 전달)
   * @param depth         현재 재귀 레벨
   * @param vertLimit     엣지 그래프를 분할하기 위한 기준 vertex 수 (이 값 보다 작거나 같으면 분할 중단)
   * @param minVectorSize 최소 벡터 사이즈 (이 값보다 벡터 사이즈가 작아지면 분할 중단)
   * @return 분할된 파티션 정보 List - [1뎁스 분할 정보, 2뎁스 분할정보, ... , n뎁스 분할 정보] 형태
   */
  def randomPartitioning(spark: SparkSession, graph: Graph[Long, Long], numVerts: Long, numPartitions: Int, vectorSize: Int, res: List[PartGraphInfo] = List[PartGraphInfo](), depth: Int = 1, vertLimit: Int = 10, minVectorSize: Int = 10): List[PartGraphInfo] = {
    if (numVerts <= vertLimit) { // 분할 대상 그래프의 노드 수가 기준치보다 작으면 종료
      val aVert = graph.vertices.take(1).head._1
      res ++ List(new PartGraphInfo(graph.mapVertices((v, x) => aVert), 1, vectorSize, depth))
    } else if (vectorSize < minVectorSize) { // 분할될 그래프의 벡터 크기가 기준치보다 작으면 종료하고, 0으로 패딩하라는 신호 추가
      val aVert = graph.vertices.take(1).head._1
      res ++ List(new PartGraphInfo(graph.mapVertices((v, x) => aVert), 1, vectorSize, depth, zeroVector = true))
    } else { // 그래프 분할 시작
      val starts = spark.sparkContext.broadcast(graph.vertices.takeSample(false, numPartitions).map(_._1).toSet) // 시작 지점을 탐색
      val initGraph = graph.mapVertices { (i, v) => // 그래프 초기화
        if (starts.value.contains(i)) { // 시작지점은 별도로 표시 (각 노드의 고유 ID를 파티션 ID로 활용)
          i.toLong
        } else // 시작지점이 아닌 지점은 -1 값을 저장 (-1이 파티션에 속해있지 않다는 의미)
          -1
      }

      // 그래프 파티셔닝 시작
      val partitionedGraph = initGraph.pregel(-1.toLong)(
        (id, cur_val, start) => {
          if (cur_val >= 0) // 이미 파티션에 속한 노드라면 해당 파티션 정보 유지
            cur_val
          else // 파티션에 속해있지 않다면 해당 지점을 현재 탐색 정보와 같은 파티션으로 지정
            start
        },
        triplet => {
          if (triplet.srcAttr < 0) { // 시작 지점이 파티션에서 속해있지 않다면 이동하지 않음
            Iterator.empty
          } else if (triplet.dstAttr < 0) { // 시작 지점이 파티션에 속해있고, 탐색 대상 지점이 파티션에 속해있지 않다면 이동
            Iterator((triplet.dstId, triplet.srcAttr))
          } else { // 탐색 대상 지점이 파티션에 속해있다면 이동하지 않음
            Iterator.empty
          }
        },
        (a, b) => if (Math.random() < 0.5) a else b // 여러 방향에서 동시에 접근시 하나의 접근만 허용
      )

      val edgeGraph = partitionedGraph.subgraph(epred = (x => x.srcAttr != x.dstAttr)) // 다른 파티션과 연결된 엣지를 찾아 해당 엣지들로만 구성된 서브그래프 (엣지 그래프) 제작
      val edgeGraphVertNum = edgeGraph.vertices.count() // 해당 엣지들에 속한 노드들의 수

      val ratio = edgeGraphVertNum.toDouble / (numVerts + edgeGraphVertNum).toDouble
      val edgeGraphVectorSize = (ratio * vectorSize).toInt // 다음 엣지 그래프의 벡터 사이즈 계산

      val newInfo = new PartGraphInfo(partitionedGraph, numPartitions, vectorSize - edgeGraphVectorSize, depth = depth) // 새로운 파티션 분할 정보 저장

      // 새로 만들어진 분할 정보와 엣지 그래프를 넘겨주면서 재귀적으로 분할 수행
      randomPartitioning(spark, edgeGraph, edgeGraphVertNum, numPartitions, edgeGraphVectorSize, res ++ List(newInfo), depth + 1)
    }
  }

  /**
   * 분할된 그래프를 하위에서부터 통합해가며 임베딩 벡터 계산
   *
   * @param spark           스파크 세션
   * @param partitionedInfo randomPartitioning 함수로부터 생성된 그래프 분할 정보
   * @param walkLength      random walk 거리
   * @param numWalks        파티션별 random walk 수
   * @param p               return 파라미터
   * @param q               inout 파라미터
   * @param lastVectorSize
   * @param res             RDD[(노드, 임베딩 벡터 - Array[Double] 형태]
   * @return
   */
  def mergePartitionedVector(spark: SparkSession, partitionedInfo: List[PartGraphInfo], walkLength: Int, numWalks: Int, p: Double = 1.0, q: Double = 1.0, lastVectorSize: Int = 0, res: RDD[(Long, Array[Double])] = null): RDD[(Long, Array[Double])] = {
    if (partitionedInfo == null || partitionedInfo.isEmpty) {
      res
    } else {
      val lastVector = partitionedInfo.last.calcNode2Vec(spark, walkLength, numWalks, p, q)

      if (res == null) {
        lastVector
      } else {
        val resizedRes = res.map { x =>
          (x._1, Array.fill(partitionedInfo.last.publicVectorSize)(0.0) ++ x._2)
        }

        val resizedLastVector = lastVector.map { x =>
          (x._1, x._2 ++ Array.fill(lastVectorSize)(0.0))
        }

        val newRes = resizedRes.union(resizedLastVector).groupBy(_._1).map { g =>
          (g._1, g._2.map(_._2).reduce((a, b) => a.zip(b).map(x => x._1 + x._2)))
        }

        mergePartitionedVector(spark, partitionedInfo.dropRight(1), walkLength, numWalks, p, q, lastVectorSize + partitionedInfo.last.publicVectorSize, newRes)
      }
    }
  }
}
