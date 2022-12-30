/**
 * author: Kim Junho
 * email: lunaticfoxy@gmail.com
 */
package com.alp.struct

/**
 * 파라미터 전달을 위한 케이스 클래스
 * 파라피터 파싱 방법은 https://github.com/aditya-grover/node2vec 참조
 * @param numVerts 노드 수
 * @param numEdges 엣지 수
 * @param numPartitions 파티션 수
 * @param vectorSize 벡터 크기
 * @param walkLength Random Walk 거리
 * @param numWalks Random Walk 수
 * @param p return 파라미터
 * @param q in-out 파라미터
 */
case class Params(numVerts: Int = 20,
                  numEdges: Int = 50,
                  numPartitions: Int = 5,
                  vectorSize: Int = 128,
                  walkLength: Int = 20,
                  numWalks: Int = 20,
                  p: Double = 1.0,
                  q: Double = 1.0)