# project

- Spark-based movie recommendation system
- **Course supported by:** [gulixueyuan](http://www.gulixueyuan.com/goods/show/208?targetId=314&preview=0)

# build process

## Phase 1: Overview of Project Architecture

## Phase 2: Basic environment construction

- Virtual machine: VMware Workstation Pro
- System: CentOS 7
- Host: Lenovo Legion R9000P2021H

Configure Mongodb

Configure Redis

Configuring Elasticsearch

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
