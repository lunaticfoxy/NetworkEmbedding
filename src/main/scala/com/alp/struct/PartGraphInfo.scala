/**
 * author: Kim Junho
 * email: lunaticfoxy@gmail.com
 */
package com.alp.struct

import org.apache.spark.graphx._
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 파티션 정보를 담기 위한 클래스
 * - 파티션 정보 저장
 * - 파티션 내부에서 일어나는 연산에 대한 함수 포함
 * @param subGraphs
 * @param partNum
 * @param vectorSize
 * @param depth
 * @param zeroVector
 * @param useZeroVector
 */
class PartGraphInfo(subGraphs: Graph[Long, Long], partNum: Int, vectorSize: Int, depth: Int, zeroVector: Boolean = false, useZeroVector: Boolean = false) {
  val publicPartNum: Int = partNum
  val publicVectorSize: Int = vectorSize
  val publicDepth: Int = depth
  val publicZeroVector: Boolean = zeroVector && useZeroVector

  /**
   * subGrpah 로부터 Node2Vec 계산을 위한 Random Walk 데이터 생성 함수
   * @param spark 스파크 세션
   * @param walkLength random walk 거리
   * @param numWalks 파티션별 random walk 수
   * @param p return 파라미터
   * @param q inout 파라미터
   * @return RDD[(파티션, 탐색경로)]
   */
  def getRandomWalks(spark: SparkSession, walkLength: Int, numWalks: Int, p: Double = 1.0, q: Double = 1.0): RDD[(Long, Array[Long])] = {
    // vertex의 연결 정보를 파악하기 위한 맵을 만들고 브로드캐스트
    val edgeMap = subGraphs.edges
      .flatMap(x => Seq((x.srcId, x.dstId), (x.dstId, x.srcId)))
      .groupBy(_._1).map(x => (x._1, x._2.map(_._2).toSet)).collect().toMap

    val bcEdgeMap = spark.sparkContext.broadcast(edgeMap)

    // 각 노드에는 (파티션, Set[(탐색 경로, 탐색 완료 여부, 확률 계산용 분모)], List[여기서 완료된 탐색 경로], 노드 탐색 횟수) 를 저장
    // 한번 들어온 경로 정보는 저장해두고 계속 활용 - Set 으로 저장하여 중복 경로는 제거하여 다양한 경로 탐색 가능하도록 구성
    val initGraph = subGraphs.subgraph(vpred = {(v, x) => bcEdgeMap.value.contains(v)})
      .mapVertices((v, x) => (x, Set[(Array[Long], Boolean, Double)](), List[Array[Long]](), 0))

    val randomWalkGraph = initGraph.pregel(List[Array[Long]]())( // 전달하는 메시지는 해당 노드를 포함한 탐색 경로
      (id, node_val, msg) => {
        val pathInfo: Set[(Array[Long], Boolean, Double)] = if(msg.isEmpty) {
          Set((Array(id), walkLength <= 1, bcEdgeMap.value(id).size.toDouble))   // 탐색 시작 지점 (메시지가 비어있음)
        } else {                                                                 // 탐색 과정 (메시지에 데이터가 들어있음)
          msg.map{path =>
            // 확률 계산을 용이하게 미리 분모를 계산해 둠 (반복 연산 최소화를 위해 미리 계산해둠)
            val inout = (bcEdgeMap.value(id) -- bcEdgeMap.value(path.last)).size.toDouble
            val preceeding = (bcEdgeMap.value(id) & bcEdgeMap.value(path.last)).size.toDouble
            val ret = 1.0
            val denom = ret / p + inout / q + preceeding

            (path, walkLength <= path.length, denom)      // 새로운 패스, 마지막 탐색 지점, 탐색 완료 여부, 계산한 확률 분모
          }.toSet
        }
        val completePath = node_val._3 ++ pathInfo.filter(_._2).map(_._1) // 탐색이 완료된 패스는 따로 저장

        (node_val._1, node_val._2 ++ pathInfo, completePath, node_val._4 + 1)                 // 현재 노드에 저장된 값 갱신
      },
      triplet => {
        if (triplet.srcAttr._1 != triplet.dstAttr._1 || triplet.srcId == triplet.dstId) {       // 다른 파티션일경우 탐색하지 않음
          Iterator.empty
        } else {
          triplet.srcAttr._2.filter(attr => !attr._2 && attr._1.length >= (triplet.srcAttr._4 - walkLength)).map{attr =>
            val path = attr._1
            val before = if(path.length <= 1) -1L else path(path.length - 2)
            val denom = attr._3

            val newPath = path ++ Array(triplet.dstId)
            val randVal = Math.random()

            val validTarget = (triplet.dstId, List(newPath))    // 다음 노드를 탐색할 경우 들어갈 데이터
            val invalidTarget = (-1L, List(Array[Long]()))      // 다음 노드를 탐색하지 않을경웅 보낼 메시지

            if(before == -1L) {                                 // 첫 노드라면 확률 계산 없이 탐색
              validTarget
            } else {                                            // 두번째 이상 탐색일 경우 확률에 따라 탐색 여부 결정
              if (bcEdgeMap.value(before).contains(triplet.dstId)) { // preceeding일 경우 탐색 확률 계산
                if ((1.0 / denom) >= randVal) {
                  validTarget
                } else {
                  invalidTarget
                }
              } else if (triplet.dstId == before) {                 // return일 경우 탐색 확률 계산
                if ((1.0 / (p * denom)) >= randVal) {
                  validTarget
                } else {
                  invalidTarget
                }
              } else {                                              // inout일 경우 탐색 확률 계산
                if ((1.0 / (q * denom)) >= randVal) {
                  validTarget
                } else {
                  invalidTarget
                }
              }
            }
          }.filter(_._1 >= 0).toArray                                    // 탐색하지 않는 대상을 제거하고 Iterator로 변경
            .sortBy(_._2.length)(Ordering[Int].reverse).take(numWalks)   // 연산시간이 증가하는것을 막기위해 어차피 최대 데이터 수인 numWalks 만큼 데이터 제한
            .toIterator
        }
      },
      (a, b) => a ++ b       // 여러 방향에서 동시에 한 노드에 접근시 하나의 Set으로 묶어 전달
    )

    // 그래프에 저장된 탐색 경로중 walkLength에 도달한 데이터를 추출하여 (파티션, 탐색 경로) 형태의 RDD 로 변환
    val randomWalkFeature = randomWalkGraph.vertices.flatMap{node =>
      node._2._3.map((node._2._1, _))
    }

    // 파티션별로 데이터 수를 numWalks 로 제한
    randomWalkFeature.groupBy(_._1).flatMap(_._2.take(numWalks))
  }

  /**
   * 서브그래프 내에서 파티션별로 속한 노드를 리턴
   * @return RDD[(파티션, 노드)]
   */
  def getVerticesInPartition(): RDD[(Long, Long)] = {
    subGraphs.vertices.map{x =>
      (x._2, x._1.toLong)
    }
  }

  /**
   * Subgraph의 각각 파티션에 대해 Node2Vec을 계산하고 묶어서 리턴
   * 학습에 활용하는 모델은 Word2Vec과 같으므로 mllib의 Word2Vec 모델을 활용
   * (Word2Vec 모델을 활용하는 아이디어는 https://github.com/aditya-grover/node2vec 에서 참조, 실제 적용 코드는 직접 구현)
   * RDD 기반 mllib 라이브러리를 활용하여 concat이 용이하도록 Vector 대신 Array 형태로 임베딩 벡터를 저장
   * @param spark 스파크 세션
   * @param walkLength random walk 거리
   * @param numWalks 파티션별 random walk 수
   * @param p return 파라미터
   * @param q inout 파라미터
   * @return RDD[(노드, 임베딩 벡터 - Array[Double] 형태]
   */
  def calcNode2Vec(spark: SparkSession, walkLength: Int, numWalks: Int, p: Double = 1.0, q: Double = 1.0): RDD[(Long, Array[Double])] = {
    val verticesInPartition = getVerticesInPartition() // 각 파티션별 포함된 노드 탐색

    if(zeroVector && useZeroVector) {
      verticesInPartition.map(x => (x._2, Array.fill(vectorSize)(0.0)))
    }
    else {
      val randomWalkData = getRandomWalks(spark, walkLength, numWalks, p, q) // 벡터 계산을 위한 랜덤워크 수행
      val partitions = randomWalkData.map(_._1).distinct.collect() // 파티션 탐색

      // 각 파티션별로 Node2Vec을 통한 임베딩 벡터 계산
      val embeddingInPartition = partitions.map { p =>

        // Word2Vec 모델에 적용할 수 있도록 데이터 형태 가공
        val randomWalkString = randomWalkData.filter(_._1 == p).map { x =>
          x._2.map(_.toString).toSeq
        }

        // Word2Vec 모델을 활용하여 모델 학습
        val word2Vec = new Word2Vec().setVectorSize(vectorSize)
        val model = word2Vec.fit(randomWalkString)

        // 학습된 모델을 통해 해당 파티션에 속한 노드의 임베딩 벡터 계산 (concat 이 쉽도록 Array 로 변환하여 저장)
        verticesInPartition.filter(_._1 == p).map { x =>
          (x._2, model.transform(x._2.toString).toArray)
        }
      }

      spark.sparkContext.union(embeddingInPartition) // 각 파티션별 별개의 RDD로 저장된 데이터를 하나의 RDD로 통합하여 리턴
    }
  }
}

