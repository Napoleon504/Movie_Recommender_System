# project

- Spark-based movie recommendation system
- **Course supported by:** [gulixueyuan](http://www.gulixueyuan.com/goods/show/208?targetId=314&preview=0)

# build process

- Due to the poor readability of github, the following steps are transferred to my [yuque](https://www.yuque.com/tsuiraku/movierecommender) for reference if necessary

## Phase 1: Overview of Project Architecture

## Phase 2: Basic environment construction

- Virtual machine: virtualbox
- System: centos7
- Tool: Finalshell
- Host: macOS Big Sur

Configure Mongodb

configure redis

Configuring Elasticsearch¶

Configure Azkaban

Configure Zookeeper

Configure Spark

Configure Kafka

Configure Flume

## Phase 3: Data Loading Service

- Module: **DataLoader**
- Function:
   - Load data into MongoDB
   - Load data into Elasticsearch

## Phase 4: Recommendation system construction

- ### Statistical recommendation calculation module

   - Module: **StatisticsRecommender**
   - Implementation: **Spark-SQL**

- ### Offline recommendation module based on collaborative filtering

   - Algorithm: **ALS**
   - Module: **OfflineRecommender**
   - Implementation: **Spark-MLlib**
  
- ### Content-based recommendation module

   - Module: **ContentRecommender**
   - Implementation: **Spark-MLlib**

- ### Real-time recommendation module

   - Module: **StreamingRecommender**
   - Implementation: **Spark-Streaming**

## Phase 5: Front-end and back-end construction

- This part does not need to be built by yourself, just use the provided **businessServer**
