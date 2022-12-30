# Network Embedding

스파크를 활용한 대용량 그래프의 계층적 임베딩 방법 구현을 위한 샘플 프로젝트

랜덤한 그래프를 만들어 테스트하는 샘플 프로젝트로, 실 사용을 위해서는 개선해야 할 부분이 많이 존재

다음 논문에서 제안한 방법을 참조하여 구현
> Large-Scale Network Embedding in Apache Spark <br>
> Wenqing Lin <br>
> Proceedings of the 27th ACM SIGKDD Conference on Knowledge Discovery & Data Mining, 2021 <br>
> https://arxiv.org/abs/2106.10620

### 사용법
#### 실행
```
spark-submit --master yarn \ 
    --deploy-mode cluster \
    --class com.alp.run.Main \ 
    networkembedding_2.12-1.0.jar \
    (추가 파라미터)
```

#### 입력 파라미터
```
--numVerts NUM_VERTS
	랜덤 생성 그래프의 노드 수 (Default 20)
--numEdges NUM_EDGES
	랜덤 생성 그래프의 엣지 수 (Default 50)
--numPartitions NUM_PARTITIONS
	레벨마다 나눌 파티션 수 (Default 5)
--vectorSize VECTOR_SIZE
	벡터 사이즈 (Default 128)
--walkLength WALK_LENGTH
	Random Walk 거리 (Default 20)
--numWalks NUM_WALKS
	Random Walk 수 (Default 20)
--p P
	return 파라미터 (Default 1.0)
--q Q
	in-out 파라미터 (Default 1.0)
```

### 구조
- project/build.properties : sbt 버전 정보 탑재
- src/main/scala/com/alp : 전체 코드 포함
    - run/Main : 스파크 작업 제출 및 기본 실행 프로세스를 담은 메인 객체
    - struct/NetworkEmbedding
      - 네트워크 임베딩에 활용하기 위한 함수를 담은 싱글톤 객체
      - 전체 그래프 대상 연산, 파티션간의 연산, 결과물 생성
  - struct/PartGraphInfo : 파티션 정보 저장 및 파티션 내 연산을 위한 클래스
- build.sbt: sbt 빌드를 위한 프로젝트 설정


### 연락 및 문의
lunaticfoxy@gmail.com (Kim Jun Ho)


### 참고 레포지토리
- https://github.com/aditya-grover/node2vec
  - Word2Vec을 활용한 Node2Vec 구현 아이디어 참고
    - 실제 코드는 별도 구현
  - 커맨드라인 파라미터 전달 구조 참고 