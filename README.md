solr-recommender
================

Simple recommender using Solr, works online with new history data in the queries. Does batch updates of the training/index data using Mahout.

## Getting Started

To compile the sources download, build, and install the lastest snapshot of Mahout from http://mahout.apache.org. Then go to the root of this project and run
```
~$ mvn clean install
```

This will build the job jar with all dependencies in ./target/solr-recommender-0.1-SNAPSHOT-job.jar. The various tasks inside the jar are described below. Each CLI accessible job comes with a fairly complete description of its parameters printed out by running with no parameters.

There is a sample script for running the job in scripts/solr-recommender. To get a trivial sample output go to the solr-recommender/scripts directory and run:
```
~$ ./solr-Recommender
```
This will use simple sample data from the project and create sample output in project root dir.

Running any job will output help, for example:
```
~$ hadoop jar ../target/solr-recommender-0.1-SNAPSHOT-job.jar \
       finderbots.recommenders.hadoop.RecommenderDriverJob
```
Prints the following:
```
 -a1 (--action1) VAL           : String respresenting action1, the primary
                                 preference action (optional). Default:
                                 'purchase'
 -a2 (--action2) VAL           : String respresenting action2, the secondary
                                 preference action (optional). Default: 'view'
 -ac (--actionColumn) N        : Which column has the action (optional).
                                 Default: 1
 -i (--input) VAL              : Input directory searched recursively for files
                                 in 'ExternalID' format where ID are unique
                                 strings and preference files contain combined
                                 actions with action IDs. Subdirs will be
                                 created by action type, so 'purchase', 'view',
                                 etc.
 -ifp (--inputFilePattern) VAL : Match this pattern when searching for action
                                 log files (optional). Default: '.tsv'
 -iidc (--itemIDColumn) N      : Which column has the itemID (optional).
                                 Default: 2
 -ix (--indexDir) VAL          : Where to put user and item indexes (optional).
                                 Default: 'id-indexes'
 -o (--output) VAL             : Output directory for recs. There will be two
                                 subdirs one for the primary recommender and
                                 one for the secondry/cross-recommender each of
                                 which will have item similarities and user
                                 history recs.
 -r (--recsPerUser) N          : Number of recommendations to return for each
                                 request. Default = 10.
 -s (--similarityType) VAL     : Similarity measure to use. Default SIMILARITY_L
                                 OGLIKELIHOOD. Note: this is only used for
                                 primary recs and secondary item similarities.
 -t (--tempDir) VAL            : Place for intermediate data. Things left after
                                 the jobs but erased before starting new ones.
 -uidc (--userIDColumn) N      : Which column has the userID (optional).
                                 Default: 0
 -x (--xRecommend)             : Create cross-recommender for multiple actions
                                 (optional). Default: false.
```
## Task Pipeline

There are sub jobs launched by some of these tasks but that detail aside there are three main tasks to run in order.
  1. Ingest text logfiles splitting into DistributedRowMatix(es) one per action
  2. Write out BiMaps that translate External string item and user IDs to and from Internal Mahout Long IDs.
  3. Calculate [B'B] with LLR using Mahout jobs to get the item-item 'similarity matrix' and write these to Solr for indexing.
  4. Calculate [A'B] using Mahout transpose and matrix multiply to get the 'cross-similarity matrix', write these to Solr for indexing
  5. Solr indexes the various fields and is ready to return raw recommendations.
  6. Queries, consisting of user history vectors of itemIDs are fed to Solr. If the primary action is being used for recommendations, the primary action field of the index is queried. If both primary (recommendations) and secondary (cross-recommendations) are desired both fields are queried. If item similarity is required, the doc associated with an item ID is returned indicating similar items. This document field will be ordered by the rank of similarity that any item has with the doc item.
  7. Solr returns a ranked list of items.

## RecommenderDriverJob

This main task fires off all the subtasks each of which is a separate CLI accessible job.
```
pat:solr-recommender pat$ hadoop jar target/recommenders-0.1-SNAPSHOT-job.jar finderbots.hadoop.RecommenderDriverJob
list of job CLI options
```
## Theory

Runs a distributed recommender and cross-recommender job as a series of mapreduces. The concept behind this is based on the fact that when preferences are taken from user actions, it is often useful to use one action for recommendation but the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased.
```
 A = matrix of action2 by user, used only in the cross-recommender
 B = matrix of action1 by user, these are the primary recommenders actions
 [B'B]H<sub>p</sub> = R<sub>p</sub>, recommendations from purchase actions with strengths
 [B'A]H_v = R_v, recommendations from view actions (where there was a purchase) with strengths
 R_p + R_v = R, assuming a non-weighted linear combination
```
The job can either pre-calculate all recs and similarities for all users and items OR it can output the similairty matrix to Solr for use as an online recommender. In this later case [B'B] and optionally [B'A] can be written so Solr. Then a user's history, as a string of item IDs, can be used as a query to return recommended items. If a specific item ID's document is fetched it will contain an ordered list of similar items.

Preferences in the input file should look like userID, action, itemID, with any other columns interspersed (and ignored).

## TBD

1. not all options are passed through the driver job to the recommender and xrecommender. These need to be checked.
2. not all options that need to be passed to the various jobs are forwarded to them, for instance recommendations and preferences per user are not forwarded, these need to be checked for completeness.
2. Solr is not integrated yet.
3. log files are of default config in the resources so other formats need to be tested.
4. the only test is to hand run and check by eyeball the bash script, this should be a junit test with output verification.
