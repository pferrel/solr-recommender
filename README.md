solr-recommender
================

Simple recommender using Solr, works online with new history data in the queries. Does batch updates of the training/index data using Mahout.

# Getting Started

To compile the sources go to the root dir and run 'mvn clean install' or if you plan to compile on a mac and run on a linux cluster run ./scripts/mvn-install This will build the job jar with all dependencies in ./target/solr-recommender-0.1-SNAPSHOT-job.jar. The various tasks inside the jar are described below. Each CLI accessible job comes with a fairly complete description of their parameters by running with no parameters.

# Task Pipeline

There are sub jobs launched by some of these tasks but that detail aside there are three main tasks to run in order.
  1. Ingest text logfiles splitting into DistributedRowMatix(es) one per action
  2. Write out BiMaps that translate External string item and user IDs to and from Internal Mahout Long IDs.
  3. Calculate [B'B] with LLR using Mahout jobs to get the item-item 'similarity matrix' and write these to Solr for indexing.
  4. Calculate [A'B] using Mahout transpose and matrix multiply to get the 'cross-similarity matrix', write these to Solr for indexing
  5. Solr indexes the various fields and is ready to return raw recommendations.
  6. Queries, consisting of user history vectors of itemIDs are fed to Solr. If the primary action is being used for recommendations, the primary action field of the index is queried. If both primary (recommendations) and secondary (cross-recommendations) are desired both fields are queried. If item similarity is required, the doc associated with an item ID is returned indicating similar items. This document field will be ordered by the rank of similarity that any item has with the doc item.
  7. Solr returns a ranked list of items.

# RecommenderDriverJob

This main task fires off all the subtasks each of which is a separate CLI accessible job.

pferrel:solr-recommender pferrel$ hadoop jar target/recommenders-0.1-SNAPSHOT-job.jar finderbots.hadoop.RecommenderDriverJob
list of job CLI options