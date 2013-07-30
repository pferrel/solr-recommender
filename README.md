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

The RecommenderDriverJob runs various subjobs, some of which can be run separately. This job will performs the following tasks:
  1. Ingest text logfiles splitting into DistributedRowMatix(es) one per action
  2. Write out BiMaps that translate External string item and user IDs to and from Internal Mahout long IDs.
  3. Calculate [B'B] with LLR using the Mahout Item-based Recommender job to get the item-item 'similarity matrix' and write these to Solr for indexing.
  4. Calculate [A'B] using Mahout transpose and matrix multiply jobs to get the 'cross-similarity matrix', write these to Solr for indexing
  4.5  Calculates all recommendations and cross-recommendations for all users using the mapreduce version of the Mahout Recommender and XRecommender. For use of Solr these are optional but may be useful to compare to Solr results.
  5. Solr indexes the various fields and is ready to return raw recommendations.
  6. Queries, consisting of user history vectors of itemIDs are fed to Solr. If the primary action is being used for recommendations, the primary action field of the index is queried. If both primary (recommendations) and secondary (cross-recommendations) are desired both fields are queried. If item similarity is required, the doc associated with an item ID is returned indicating similar items. This document field will be ordered by the rank of similarity that any item has with the doc item.
  7. Solr returns a ranked list of items.

## RecommenderDriverJob

This main task fires off all the subtasks each of which is a separate CLI accessible job.
```
pat:solr-recommender pat$ hadoop jar target/recommenders-0.1-SNAPSHOT-job.jar finderbots.hadoop.RecommenderDriverJob
list of job CLI options
```

## RecommenderDriverJob Output

The job takes a directory, which is searched recursively for files matching the pattern passed in (only substring match for now). The output will be a preference file per action type, a similairty matrix (two if doing --xrecommend), pre-calculated recs (also cross-recs if doing --xrecommend). The output is structured:
```
output
  |-- id-indexes
  |     |-- item-index serialized BiHashMap of items for all actions and users, externalIDString <-> internalIDInt
  |     |-- user-index serialized BiHashMap of users for all actions and items, externalIDString <-> internalIDInt
  |     |-- num-items.bin total number of items for all actions
  |     \-- num-users.bin total number of users for all actions
  |-- prefs
  |     |-- 'action1'
  |     |     \-- 'action1.[tsv|csv]' contains preferences from action1 with Mahout IDs = ints. These IDs
  |     |         are indexes into the ID BiMap index. They will return the string used as item or
  |     |         user ID from the original logs files. The file is input to the Mahout RecommenderJob.
  |     |-- 'action2'
  |     |      \-- 'action1.[tsv|csv]' contains preferences from action1 with Mahout IDs = ints. These IDs
  |     |         are indexes into the ID BiMap index. They will return the string used as item or
  |     |         user ID from the original logs files. The file is input to the  XRecommenderJob.
  |     \-- other
  |            \-- other.[tsv|csv] contains preferences from actions other than action1 or action2
  |-- p-recs
  |     |-- recs
  |     |     \-- part-xxxx sequence files containing Key = org.apache.mahout.math.VarLongWritable, Value = org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable. Key = userID, Value a weighted set of ItemIDs
  |     \-- sims
  |           \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable.
  |     |         This is the item similarity matrix, Key = itemID, Value a weighted set of ItemIDs
  |     |         indicating strength of similairty.
  \-- s-recs
        |-- recs
        |     \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable
        |         This is the user recommendation matrix, Key = itemID, Value a weighted set of ItemIDs
        |         indicating strength of similairty.
        \-- sims
              \-- part-xxxx sequence files making up a DistributedRowMatrix of Key = org.apache.hadoop.io.IntWritable, Value = org.apache.mahout.math.VectorWritable
                  This is the item cross-similarity matrix, Key = itemID, Value a weighted set of ItemIDs
                  indicating strength of similairty.
```
## Theory

Runs a distributed recommender and cross-recommender job as a series of mapreduces. The concept behind this is based on the fact that when preferences are taken from user actions, it is often useful to use one action for recommendation but the other will also work if the secondary action co-occurs with the first. For example views are predictive of purchases if the viewed item was indeed purchased.
```
 A = matrix of action2 by user, used only in the cross-recommender
 B = matrix of action1 by user, these are the primary recommenders actions
 [B'B]H_p = R_p, recommendations from action1
 [B'A]H_v = R_v, recommendations from action2 (where there was also an action1)
 R_p + R_v = R, assumes a non-weighted linear combination, ideally they are weighted to optimize results.
```
The job can either pre-calculate all recs and similarities for all users and items OR it can output the similairty matrix to Solr for use as an online recommender. In this later case [B'B] and optionally [B'A] can be written so Solr. Then a user's history, as a string of item IDs, can be used as a query to return recommended items. If a specific item ID's document is fetched it will contain an ordered list of similar items.

Preferences in the input file should look like userID, action, itemID, with any other columns interspersed (and ignored).

## TBD

Happy path works, creating the two similarity matrixes for moving to Solr, but many other options are not yet supported or tested.

1. Solr is not integrated yet.
2. not all options are accepted by the main driver nor are they forwarded to the sub jobs properly. These need to be checked.
3. input log files are of default config in the resources so other formats need to be tested.
4. the only test is to hand run and check by eyeball using the supplied the bash script, this should be a unit test with output verification.
